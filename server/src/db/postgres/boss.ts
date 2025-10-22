import PgBoss from "pg-boss";
import dotenv from "dotenv";

dotenv.config();

const boss = new PgBoss({
  host: process.env.POSTGRES_HOST || "postgres",
  port: parseInt(process.env.POSTGRES_PORT || "5432", 10),
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  schema: "pgboss",
  application_name: "data-import-system",
  retryLimit: 0,
});

boss.on("error", error => console.error(error));

export default boss;
