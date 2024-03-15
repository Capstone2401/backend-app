"use strict";

const { Client } = require("pg");
const config = require("./config");

const logQuery = (statement, parameters) => {
  let timeStamp = new Date();
  let formattedTimeStamp = timeStamp.toString().substring(4, 24);
  console.log(formattedTimeStamp, statement, parameters);
};

const dbQuery = async (statement, ...parameters) => {
  try {
    let client = new Client({ connectionString: config.REDSHIFT_CONN_STRING });
    await client.connect();

    logQuery(statement, parameters);

    let result = await client.query(statement, parameters);
    await client.end();

    return result;
  } catch (error) {
    console.error(error);
  }
};

// dbQuery("SELECT * FROM events").then((result) =>
//   console.log(JSON.stringify(result.rows[0])),
// );
//
module.exports = dbQuery;
