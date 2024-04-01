import { Client } from "pg";
import { QueryResult } from "pg";
import config from "./config";

function logQuery(statement: string, parameters: string[]): void {
  let timeStamp = new Date();
  let formattedTimeStamp = timeStamp.toString().substring(4, 24);
  console.log(formattedTimeStamp, statement, parameters);
}

async function dbQuery(
  statement: string,
  ...parameters: string[]
): Promise<QueryResult | Error> {
  try {
    let client = new Client({ connectionString: config.REDSHIFT_CONN_STRING });
    await client.connect();

    logQuery(statement, parameters);

    let result = await client.query(statement, parameters);
    await client.end();

    return result;
  } catch (error) {
    if (error instanceof Error) {
      return error;
    }
  }

  return new Error("Unknown error occured");
}

module.exports = dbQuery;
