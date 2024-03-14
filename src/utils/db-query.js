"use strict";

const { Client } = require("pg");

const logQuery = (statement, parameters) => {
  let timeStamp = new Date();
  let formattedTimeStamp = timeStamp.toString().substring(4, 24);
  console.log(formattedTimeStamp, statement, parameters);
};

const dbQuery = async (statement, ...parameters) => {
  let client = new Client({ connectionString: "" }); // Will Still need to be configured to work with Redshift.

  await client.connect();
  logQuery(statement, parameters);
  let result = await client.query(statement, parameters);
  await client.end();

  return result;
};

module.exports = dbQuery;
