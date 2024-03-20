"use strict";

require("dotenv").config();
const express = require("express");
const config = require("./utils/config");
const morgan = require("morgan");
const redshift = require("./lib/redshift-pg");

const app = express();

const host = config.HOST || "localhost";
const port = config.PORT || 3000;

app.use(morgan("common"));

app.get("/", (_req, res) => res.send("hello world"));

app.get("/test", async (_req, res) => {
  const result = await redshift.events.listAll();
  res.status(200).send(JSON.stringify(result));
});

app.get("/data", (_req, _res, next) => {
  try {
    // STUB
  } catch (error) {
    next(error);
  }
});

// Error handler
app.use((err, _req, res, _next) => {
  console.error(err); // Writes more extensive information to the console log
  res.status(404).send(err.message); // Tranform to more granular error messages for 4XX and 5XX
});

// Listener
app.listen(port, "0.0.0.0", () => {
  console.log(`App is listening on port ${port} of ${host}.`);
});
