import { Client } from "pg";
import { QueryResult } from "pg";
import { ResponseError } from "./response-error";
import config from "./config";
import log from "./log";

function logQuery(
  statement: string,
  parameters: Array<string | null | undefined>,
): void {
  let timeStamp = new Date();
  let formattedTimeStamp = timeStamp.toString().substring(4, 24);
  log.info(formattedTimeStamp, statement, parameters);
}

async function dbQuery(
  statement: string,
  ...parameters: Array<string | null | undefined>
): Promise<QueryResult> {
  try {
    let client = new Client({ connectionString: config.REDSHIFT_CONN_STRING });
    await client.connect();

    logQuery(statement, parameters);

    // @ts-ignore
    let result = await client.query(statement, parameters);
    await client.end();

    return result;
  } catch (error) {
    if (error instanceof Error) {
      log.error(new Error(error.message));
      throw new ResponseError({
        message: "There was an unexpcted error when trying to query your data.",
        statusCode: 500,
      });
    }
  }

  const error = new Error(
    "An unknown error occured when trying to query Redshift with pg.",
  );

  log.error(error);

  throw new ResponseError({
    message: "An unexpcted error occured.",
    statusCode: 500,
  });
}

export default dbQuery;
