import Bull from "bull";
import { fromPairs } from "lodash";
import type { Context, Service, ServiceSchema, ServiceSettingSchema } from "moleculer";
import { Pool } from "pg";
import format from "pg-format";

/* eslint-disable @typescript-eslint/no-floating-promises */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */

interface GreeterSettings extends ServiceSettingSchema {
	defaultName: string;
}

interface GreeterMethods {
	uppercase(str: string): string;
}

interface GreeterLocalVars {
	myVar: string;
}

type GreeterThis = Service<GreeterSettings> & GreeterMethods & GreeterLocalVars;

/* ------------------------------------------------------------------
   PostgreSQL Pool
------------------------------------------------------------------- */
const pool = new Pool({
	user: process.env.PG_USER,
	password: process.env.PG_PASSWORD,
	host: process.env.PG_HOST,
	port: process.env.PG_PORT ? Number(process.env.PG_PORT) : 5432,
	database: process.env.PG_DATABASE,
	max: 20,
	idleTimeoutMillis: 30000,
	connectionTimeoutMillis: 2000,
});

/* ------------------------------------------------------------------
   Bull Queue (with retries & backoff)
------------------------------------------------------------------- */
const fhirQueue = new Bull<{
	data: { encounters: string[][]; patients: string[][] };
}>("fhir", {
	redis: {
		host: process.env.REDIS_HOST || "127.0.0.1",
		port: Number(process.env.REDIS_PORT) || 6379,
	},
	defaultJobOptions: {
		attempts: 3,
		backoff: {
			type: "exponential",
			delay: 5000,
		},
		removeOnComplete: true,
		removeOnFail: false,
	},
});

/* ------------------------------------------------------------------
   Queue Worker (DB INSERTS)
------------------------------------------------------------------- */
const insert = async ({
						  data,
					  }: {
	data: { encounters: string[][]; patients: string[][] };
}) => {
	const connection = await pool.connect();

	try {
		await connection.query("BEGIN");

		if (data.patients.length > 0) {
			await connection.query(
				format(
					`INSERT INTO staging_patient
					 (case_id, sex, date_of_birth, deceased, date_of_death, facility_id,
					  patient_clinic_no, patient_name, phone_number, country, district,
					  subcounty, parish, village, national_id)
					 VALUES %L
					 ON CONFLICT (case_id) DO UPDATE
												  SET sex = EXCLUDED.sex,
												  date_of_birth = EXCLUDED.date_of_birth,
												  deceased = EXCLUDED.deceased,
												  date_of_death = EXCLUDED.date_of_death,
												  facility_id = EXCLUDED.facility_id,
												  patient_clinic_no = EXCLUDED.patient_clinic_no,
												  patient_name = EXCLUDED.patient_name,
												  phone_number = EXCLUDED.phone_number,
												  country = EXCLUDED.country,
												  district = EXCLUDED.district,
												  subcounty = EXCLUDED.subcounty,
												  parish = EXCLUDED.parish,
												  village = EXCLUDED.village,
												  national_id = EXCLUDED.national_id,
												  updated_date = current_timestamp`,
					data.patients
				)
			);
		}

		if (data.encounters.length > 0) {
			await connection.query(
				format(
					`INSERT INTO staging_patient_encounters
					 (case_id, encounter_id, encounter_date, facility_id, encounter_type, obs)
					 VALUES %L
					 ON CONFLICT (encounter_id) DO UPDATE
					 SET case_id = EXCLUDED.case_id,
					     encounter_date = EXCLUDED.encounter_date,
					     facility_id = EXCLUDED.facility_id,
					     encounter_type = EXCLUDED.encounter_type,
					     obs = EXCLUDED.obs,
					     updated_date = current_timestamp`,
					data.encounters
				)
			);
		}

		await connection.query("COMMIT");
	} catch (error) {
		await connection.query("ROLLBACK");
		console.error("FHIR DB insert failed:", error);

		// 🔥 REQUIRED: mark job as FAILED
		throw error;
	} finally {
		connection.release();
	}
};

/* Register worker */
fhirQueue.process((job) => insert(job.data));

/* ------------------------------------------------------------------
   Moleculer Service
------------------------------------------------------------------- */
const GreeterService: ServiceSchema<GreeterSettings> = {
	name: "fhir",

	settings: {
		defaultName: "Fhir",
	},

	actions: {
		/* ----------------------------------------------------------
		   INGEST FHIR BUNDLE
		----------------------------------------------------------- */
		add: {
			rest: {
				method: "POST",
				path: "/",
			},
			async handler(this: GreeterThis, ctx: Context<Record<string, any>>) {
				let allPatients: any[] = [];
				let allObservations: any[] = [];
				let allEncounters: any[] = [];

				ctx.params.entry.forEach((entry: any) => {
					if (entry.resource?.resourceType === "Patient")
						allPatients.push(entry);
					if (entry.resource?.resourceType === "Encounter")
						allEncounters.push(entry);
					if (entry.resource?.resourceType === "Observation")
						allObservations.push(entry);
				});

				const patients = this.processPatients(allPatients);
				const encountersRaw = this.processEncounters(allEncounters);
				const observations = this.processObs(allObservations);

				const encounters = encountersRaw.map((e: any[]) => {
					const encounterId = e[1];
					const encounterObs = fromPairs(
						observations
							.filter((o: { encounterId: any }) => o.encounterId === encounterId)
							.map((o: any) => [o.obs_name, o]),
					);
					return [...e, JSON.stringify(encounterObs)];
				});

				const job = await fhirQueue.add({
					data: { patients, encounters },
				});

				// ✅ Correct async response
				(ctx.meta as any).$statusCode = 202;

				return {
					status: "ACCEPTED",
					jobId: job.id,
					message: "FHIR bundle queued for processing",
				};
			},
		},

		/* ----------------------------------------------------------
		   JOB STATUS
		----------------------------------------------------------- */
		getJobStatus: {
			rest: {
				method: "GET",
				path: "/jobs/:id",
			},
			async handler(ctx) {
				const job = await fhirQueue.getJob(ctx.params.id);
				if (!job) return { status: "NOT_FOUND" };

				return {
					id: job.id,
					state: await job.getState(),
					failedReason: job.failedReason,
				};
			},
		},
	},

	/* ------------------------------------------------------------------
	   Helper methods (unchanged logic)
	------------------------------------------------------------------- */
	methods: {
		processPatients(patients) {
			const processed = [];
			for (const patient of patients) {
				const r = patient.resource;
				if (!r?.id || !r.gender || !r.birthDate || !r.managingOrganization)
					continue;

				processed.push([
					r.id,
					r.gender,
					r.birthDate.length === 4 ? `${r.birthDate}-01-01` : r.birthDate,
					r.deceasedBoolean,
					r.deceasedDateTime || null,
					String(r.managingOrganization.reference).split("/")[1],
					null,
					`${r.name?.[0]?.given?.[0] || ""} ${r.name?.[0]?.family || ""}`.trim(),
					r.telecom?.[0]?.value || null,
					r.address?.[0]?.country || null,
					r.address?.[0]?.district || null,
					null,
					null,
					null,
					null,
				]);
			}
			return processed;
		},

		processEncounters(encounters) {
			const processed = [];
			for (const e of encounters) {
				const r = e.resource;
				if (!r?.id || !r.period?.start || !r.subject || !r.serviceProvider)
					continue;

				processed.push([
					String(r.subject.reference).split("/")[1],
					r.id,
					r.period.start,
					String(r.serviceProvider.reference).split("/")[1],
					r.type?.[0]?.coding?.[0]?.code || null,
				]);
			}
			return processed;
		},

		processObs(observations) {
			const obs = [];
			for (const o of observations) {
				const r = o.resource;
				if (!r?.encounter || !r?.subject || !r?.code) continue;

				const encounterId = String(r.encounter.reference).split("/")[1];
				const patientId = String(r.subject.reference).split("/")[1];
				const coding = r.code.coding?.[0];

				obs.push({
					id: r.id,
					patient: patientId,
					encounterId,
					obs_name: coding?.display,
					code: coding?.code,
					realValue:
						r.valueString ??
						r.valueBoolean ??
						r.valueInteger ??
						r.valueDateTime ??
						r.valueQuantity?.value,
				});
			}
			return obs;
		},
	},
};

export default GreeterService;
