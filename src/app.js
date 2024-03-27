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

app.get("/allEventNames", async (_req, res, next) => {
  try {
    let result = await redshift.getAllEventNames();
    result = result.map((entry) => entry.event_name);
    res.set("Access-Control-Allow-Origin", "*");
    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
});

app.get("/events", (req, res, next) => {
  const TIMEUNIT_BY_RANGE = {
    Today: "hour",
    Yesterday: "hour",
    "7D": "day",
    "30D": "day",
    "3M": "month",
    "6M": "month",
    "12M": "month",
  };

  const PREVIOUS_BY_RANGE = {
    Today: 24,
    Yesterday: 48,
    "7D": 7,
    "30D": 30,
    "3M": 3,
    "6M": 6,
    "12M": 12,
  };

  let { dateRange, eventName, aggregationType } = req.query;
  try {
    let result = redshift.getAggregatedEventsBy(
      TIMEUNIT_BY_RANGE[dateRange],
      aggregationType,
      {
        previous: PREVIOUS_BY_RANGE[dateRange],
        eventName,
      },
    );

    res.set("Access-Control-Allow-Origin", "*");
    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
});

// Error handler
app.use((err, _req, res, _next) => {
  console.error(err); // Writes more extensive information to the console log
  res.status(404).send(err.message); // TODO; Tranform to more granular error messages for 4XX and 5XX
});

// Listener
app.listen(port, host, () => {
  console.log(`App is listening on port ${port} of ${host}.`);
});
