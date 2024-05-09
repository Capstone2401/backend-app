import { config } from "dotenv";
config();

const HOST = process.env.host || "0.0.0.0";
const PORT = Number(process.env.port) || 3000;
const REDSHIFT_CONN_STRING = process.env.REDSHIFT_CONN_STRING || "";

export default {
  HOST,
  PORT,
  REDSHIFT_CONN_STRING,
};
