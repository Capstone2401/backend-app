"use strict";

const dbQuery = require("../utils/db-query");
const events = require("../sql/events");
const users = require("../sql/users");
const { VALID_TIME_UNIT } = require("../lib/globals");
const formatDataBy = require("../utils/format-records");
const formatAttributes = require("../utils/format-attributes");

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

async function getAggregatedUsersBy(timeUnit, aggregationType, options) {
  if (!AGGREGATE_USERS[aggregationType]) return "Invalid aggregation provided";
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  const { previous } = options;
  let dateRangeStart;

  if (previous) {
    dateRangeStart = ADJUST_DATE[timeUnit](previous, new Date());
  }

  const { eventName, filters } = options;

  const result = await dbQuery(
    AGGREGATE_USERS[aggregationType](timeUnit),
    dateRangeStart,
    eventName,
    filters ? filters.events : {},
    filters ? filters.users : {},
  );

  return formatDataBy(timeUnit, result.rows, previous);
}

async function getAggregatedEventsBy(timeUnit, aggregationType, options) {
  if (!AGGREGATE_EVENTS[aggregationType]) return "Invalid aggregation provided";
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  const { previous } = options;
  let dateRangeStart;

  if (previous) {
    dateRangeStart = ADJUST_DATE[timeUnit](previous, new Date());
  }

  const { eventName, filters } = options;

  const result = await dbQuery(
    AGGREGATE_EVENTS[aggregationType](timeUnit),
    dateRangeStart,
    eventName,
    filters ? filters.events : {},
    filters ? filters.users : {},
  );

  return formatDataBy(timeUnit, result.rows, previous);
}

async function getAllEventNames() {
  let result;

  try {
    result = await dbQuery(events.getAllEventNames());
  } catch (error) {
    console.error(error);
  }

  return result.rows;
}

async function getAllAttributes() {
  let result;

  try {
    let eventAttributes = dbQuery(events.getAllEventAttributes());
    let userAttributes = dbQuery(users.getAllUserAttributes());
    result = await Promise.all([eventAttributes, userAttributes]);
    let formattedResult = formatAttributes(result);
    return formattedResult;
  } catch (error) {
    console.error(error);
  }
}

module.exports = {
  getAllEventNames,
  getAggregatedEventsBy,
  getAggregatedUsersBy,
  getAllAttributes,
};
