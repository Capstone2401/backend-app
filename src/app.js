"use strict";

const express = require("express");
const config = require("./utils/config");
const morgan = require("morgan");
const queryRouter = require("./routes/query");
const infoRouter = require("./routes/info");
const { catchAllErrorHandler } = require("./middleware/errors");

const app = express();
app.use(morgan("common"));

const host = config.HOST || "localhost";
const port = config.PORT || 3000;

// routes
app.use("/api/query", queryRouter);
app.use("/api/info", infoRouter);

app.get("/users", (req, res, next) => {
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
    let result = redshift.getAggregatedUsersBy(
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
app.use(catchAllErrorHandler);

// Listener
app.listen(port, host, () => {
  console.log(`App is listening on port ${port} of ${host}.`);
});
