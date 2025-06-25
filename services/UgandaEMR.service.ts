import { ServiceSchema, Context } from "moleculer";
import { Pool } from "pg";

const pool = new Pool({
	user: process.env.PG_USER_DWH,
	password: process.env.PG_PASSWORD_DWH,
	host: process.env.PG_HOST_DWH,
	port: process.env.PG_PORT ? Number(process.env.PG_PORT) : 5432,
	database: process.env.PG_DATABASE_DWH,
	max: 20,
	idleTimeoutMillis: 30000,
	connectionTimeoutMillis: 2000,
});

// Define the shape of your meta
interface CustomMeta {
	source?: string;
}

const RawFhirService: ServiceSchema = {
	name: "raw.fhir",

	actions: {
		save: {
			rest: {
				method: "POST",
				path: "/",
			},
			async handler(ctx: Context<Record<string, any>, CustomMeta>) {
				const rawFhirBundle = ctx.params;

				const now = new Date();

				try {
					await pool.query(
						`INSERT INTO "import".message_resource (
							resource_published,
							resource_updated,
							resource_data,
							source_system
						) VALUES ($1, $2, $3, $4)`,
						[
							now,
							now,
							JSON.stringify(rawFhirBundle),
							ctx.meta?.source || "UgandaEMR"
						]
					);
					return { success: true, message: "FHIR bundle saved successfully." };
				} catch (error) {
					this.logger.error("Error saving FHIR bundle", error);
					return { success: false, error: error.message };
				}
			},
		},
	},
};

export default RawFhirService;
