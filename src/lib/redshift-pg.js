"use strict";

const dbQuery = require("../utils/db-query");
const events = require("../sql/events");
const users = require("../sql/users");
const { VALID_TIME_UNIT } = require("../lib/globals");
const formatDataBy = require("../utils/format-records");

const AGGREGATE_EVENTS = {
  total: events.getTotalEventsBy,
  average: events.getAveragePerUserBy,
  median: events.getMedianPerUserBy,
  minimum: events.getMinPerUserBy,
  maximum: events.getMaxPerUserBy,
};

const AGGREGATE_USERS = {
  total: users.getTotalUsersBy,
};

const ADJUST_DATE = {
  hour: (offset, currDate) => {
    currDate.setHours(currDate.getHours() - offset);
    return currDate;
  },
  day: (offset, currDate) => {
    currDate.setDate(currDate.getDate() - offset);
    return currDate;
  },
  month: (offset, currDate) => {
    currDate.setMonth(currDate.getMonth() - offset);
    return currDate;
  },
};

async function getAggregatedUsersBy(timeUnit, aggregationType, data) {
  if (!AGGREGATE_USERS[aggregationType]) return "Invalid aggregation provided";
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  const { previous } = data;
  let dateRangeStart;

  if (previous) {
    dateRangeStart = ADJUST_DATE[timeUnit](previous, new Date());
  }

  const { event_name, filterAttribute, filterAttributeValue } = data;

  const result = await dbQuery(
    AGGREGATE_USERS[aggregationType](timeUnit),
    dateRangeStart,
    event_name,
    filterAttribute,
    filterAttributeValue,
  );

  return formatDataBy(timeUnit, result.rows, previous);
}

async function getAggregatedEventsBy(timeUnit, aggregationType, data) {
  if (!AGGREGATE_EVENTS[aggregationType]) return "Invalid aggregation provided";
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  const { previous } = data;
  let dateRangeStart;

  if (previous) {
    dateRangeStart = ADJUST_DATE[timeUnit](previous, new Date());
  }

  const { event_name, filterAttribute, filterAttributeValue } = data;

  const result = await dbQuery(
    AGGREGATE_EVENTS[aggregationType](timeUnit),
    dateRangeStart,
    event_name,
    filterAttribute,
    filterAttributeValue,
  );

  return formatDataBy(timeUnit, result.rows, previous);
}

async function getAllEventNames() {
  let result;

  try {
    result = await dbQuery();
  } catch (error) {
    console.error(error);
  }

  return result.rows;
}

module.exports = {
  getAllEventNames,
  getAggregatedEventsBy,
  getAggregatedUsersBy,
};
