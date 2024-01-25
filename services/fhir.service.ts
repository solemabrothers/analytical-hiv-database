/* eslint-disable @typescript-eslint/no-floating-promises */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
import Bull from "bull";
import { fromPairs } from "lodash";
import type {
	Context,
	LoggerInstance,
	Service,
	ServiceSchema,
	ServiceSettingSchema,
} from "moleculer";
import { Pool } from "pg";
import format from "pg-format";

export interface ActionHelloParams {
	name: string;
}

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

const fhirQueue = new Bull<{
	data: { encounters: string[][]; patients: string[][] };
	logger: LoggerInstance;
}>("fhir");

const insert = async ({
	data,
	logger,
}: {
	data: { encounters: string[][]; patients: string[][] };
	logger: LoggerInstance;
}) => {
	const connection = await pool.connect();
	try {
		const r1 = await connection.query(
			format(
				`INSERT INTO staging_patient (case_id,sex,date_of_birth,deceased,date_of_death,facility_id,patient_clinic_no) VALUES %L ON CONFLICT (case_id) DO UPDATE SET sex = EXCLUDED.sex,date_of_birth = EXCLUDED.date_of_birth,deceased = EXCLUDED.deceased,date_of_death = EXCLUDED.date_of_death,facility_id = EXCLUDED.facility_id,patient_clinic_no = EXCLUDED.patient_clinic_no;`,
				data.patients,
			),
		);
		logger.info(r1.rowCount);
		const r2 = await connection.query(
			format(
				"INSERT INTO staging_patient_encounters(case_id,encounter_id,encounter_date,facility_id,encounter_type,obs) VALUES %L ON CONFLICT (encounter_id) DO UPDATE SET case_id = EXCLUDED.case_id,encounter_date = EXCLUDED.encounter_date,facility_id = EXCLUDED.facility_id,encounter_type = EXCLUDED.encounter_type,ob=EXCLUDED.obs",
				data,
			),
		);
		logger.info(r2.rowCount);
	} catch (error) {
		logger.error(error.message);
	} finally {
		connection.release();
	}
};

fhirQueue.process((job) => insert(job.data));
const GreeterService: ServiceSchema<GreeterSettings> = {
	name: "fhir",

	/**
	 * Settings
	 */
	settings: {
		defaultName: "Fhir",
	},

	/**
	 * Dependencies
	 */
	dependencies: [],

	/**
	 * Actions
	 */
	actions: {
		add: {
			rest: {
				method: "POST",
				path: "/",
			},
			handler(this: GreeterThis, ctx: Context<Record<string, any>>) {
				const patients: string[][] = this.processPatients(
					ctx.params.entry.filter(
						(entry: any) => entry.resource.resourceType === "Patient",
					),
				);
				const encounters: string[][] = this.processEncounters(
					ctx.params.entry.filter(
						(entry: any) => entry.resource.resourceType === "Encounter",
					),
				);
				const observations: any[] = this.processObs(
					ctx.params.entry.filter(
						(entry: any) => entry.resource.resourceType === "Observation",
					),
				);
				return fhirQueue.add(
					{
						data: {
							patients,
							encounters: encounters.map((e) => {
								const encounterId = e[1];
								this.logger.info(encounterId);
								const encounterObs = fromPairs(
									observations
										.filter((o) => o.encounterId === encounterId)
										.map((currentObs: any) => [
											currentObs.obs_name,
											currentObs,
										]),
								);
								return [...e, JSON.stringify(encounterObs)];
							}),
						},
						logger: this.logger,
					},
					{ priority: 1 },
				);
			},
		},
	},

	/**
	 * Events
	 */
	events: {},

	/**
	 * Methods
	 */
	methods: {
		processPatients(patients) {
			const processedPatient = [];
			for (const patient of patients) {
				let patientInfo = {
					case_id: patient.resource.id,
					sex: patient.resource.gender,
					date_of_birth: patient.resource.birthDate,
					deceased: patient.resource.deceasedBoolean,
					date_of_death: patient.resource.deceasedDateTime || null,
					facility_id: "",
					patient_clinic_number: null,
				};

				if (patient.resource.identifier) {
					const patientClinicNo = patient.resource.identifier.find(
						({ type: { text } }: { type: { text: string } }) =>
							text === "HIV Clinic No.",
					);
					if (patientClinicNo) {
						patientInfo = {
							...patientInfo,
							patient_clinic_number: patientClinicNo.value,
						};
					}
				}

				if (patient.resource.managingOrganization) {
					patientInfo = {
						...patientInfo,
						facility_id: String(patient.resource.managingOrganization.reference).split(
							"/",
						)[1],
					};
				}
				if (patientInfo.date_of_birth && patientInfo.date_of_birth.length === 4) {
					patientInfo = {
						...patientInfo,
						date_of_birth: `${patientInfo.date_of_birth}-01-01`,
					};
				}
				if (
					patientInfo.case_id &&
					patientInfo.date_of_birth &&
					patientInfo.date_of_birth.length === 10 &&
					patientInfo.sex &&
					patientInfo.facility_id
				) {
					processedPatient.push([
						patientInfo.case_id,
						patientInfo.sex,
						patientInfo.date_of_birth,
						patientInfo.deceased,
						patientInfo.date_of_death,
						patientInfo.facility_id,
						patientInfo.patient_clinic_number,
					]);
				}
			}
			return processedPatient;
		},

		processObs(observations) {
			const obs = [];
			if (observations && observations.length > 0) {
				for (const bundle of observations) {
					const {
						id,
						valueQuantity,
						valueCodeableConcept,
						valueString,
						valueBoolean,
						valueInteger,
						valueTime,
						valueDateTime,
						encounter: { reference: ref },
						effectiveDateTime,
						code: {
							coding: [{ display: obsName, code }, { code: code2 }],
						},
						subject: { reference },
					} = bundle.resource;
					let realValue =
						valueString || valueBoolean || valueInteger || valueTime || valueDateTime;

					if (valueQuantity) {
						realValue = valueQuantity.value;
					}
					if (valueCodeableConcept) {
						const {
							coding: [{ display }],
						} = valueCodeableConcept;
						realValue = display;
					}
					const patient = String(reference).split("/")[1];
					const encounterId = String(ref).split("/")[1];
					if (realValue) {
						obs.push({
							id,
							patient,
							encounterId,
							code: code2,
							uuid: code,
							obs_name: obsName,
							realValue,
							effectiveDateTime,
						});
					}
				}
			}
			return obs;
		},

		processEncounters(encounters) {
			const processed = [];
			if (encounters && encounters.length > 0) {
				for (const bundle of encounters) {
					const { id, type, period, subject, serviceProvider } = bundle.resource;
					if (type && type.length > 0 && id && period && subject && serviceProvider) {
						const [
							{
								coding: [{ code }],
							},
						] = type;
						const { start: encounterDate } = period;
						const { reference } = subject;
						const { reference: facility } = serviceProvider;
						const patientId = String(reference).split("/")[1];
						const facilityId = String(facility).split("/")[1];
						processed.push([patientId, id, encounterDate, facilityId, code]);
					}
				}
			}
			return processed;
		},
	},

	/**
	 * Service created lifecycle event handler
	 */
	created() {},

	/**
	 * Service started lifecycle event handler
	 */
	async started() {},

	/**
	 * Service stopped lifecycle event handler
	 */
	async stopped() {},
};

export default GreeterService;
