"use strict";

const express = require("express");
const config = require("./utils/config");
const morgan = require("morgan");

const app = express();
const host = config.HOST || "localhost";
const port = config.PORT || 3000;

app.use(morgan("common"));

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
app.listen(port, host, () => {
  console.log(`App is listening on port ${port} of ${host}.`);
});
