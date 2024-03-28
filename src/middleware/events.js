const redshift = require("../lib/redshift-pg");

async function handleQueryEventNames(_req, res, next) {
  try {
    let result = await redshift.getAllEventNames();
    result = result.map((entry) => entry.event_name);
    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
}

async function handleQueryEvents(req, res, next) {
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
    let result = await redshift.getAggregatedEventsBy(
      TIMEUNIT_BY_RANGE[dateRange],
      aggregationType,
      {
        previous: PREVIOUS_BY_RANGE[dateRange],
        eventName,
      },
    );

    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
}

module.exports = {
  handleQueryEventNames,
  handleQueryEvents,
};
