import dbQuery from "../utils/db-query";
import events from "../sql/events";
import users from "../sql/users";
import { VALID_TIME_UNIT } from "../lib/globals";
import formatDataBy from "../utils/format-records";
import formatAttributes from "../utils/format-attributes";

import { QueryResultRow } from "pg";
import { QueryArguments } from "../../types/query-types";
import { AggregateEvents, AggregateUsers } from "../../types/redshift-types";
import { DateMap, DateOffsetMethod } from "../../types/time";

const AGGREGATE_EVENTS: AggregateEvents = {
  total: events.getTotalEventsBy,
  average: events.getAveragePerUserBy,
  median: events.getMedianPerUserBy,
  minimum: events.getMinPerUserBy,
  maximum: events.getMaxPerUserBy,
};

const AGGREGATE_USERS: AggregateUsers = {
  total: users.getTotalUsersBy,
};

type AdjustDate = DateMap<DateOffsetMethod>;

const ADJUST_DATE: AdjustDate = {
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

async function getAggregatedUsersBy({
  timeUnit,
  aggregationType,
  options,
}: QueryArguments) {
  if (!(aggregationType in AGGREGATE_USERS)) {
    return new Error("Invalid aggregation provided");
  }

  if (!VALID_TIME_UNIT[timeUnit]) {
    return new Error("Invalid time unit provided");
  }

  let previous;
  let filters;
  let eventName;

  if (options) {
    previous = options.previous;
    filters = options.filters;
    eventName = options.eventName;
  }

  let dateRangeStart;

  if (!previous) {
    previous = 0;
  }

  dateRangeStart = ADJUST_DATE[timeUnit as keyof AdjustDate](
    previous,
    new Date(),
  );

  const result = await dbQuery(
    AGGREGATE_USERS[aggregationType as keyof AggregateUsers](timeUnit),
    dateRangeStart?.toISOString(),
    eventName,
    filters ? JSON.stringify(filters.events) : "{}",
    filters ? JSON.stringify(filters.users) : "{}",
  );

  if (result instanceof Error) {
    return Error;
  }

  const records: QueryResultRow[] = result.rows;
  return formatDataBy(timeUnit, records, previous);
}

async function getAggregatedEventsBy({
  timeUnit,
  aggregationType,
  options,
}: QueryArguments) {
  if (!(aggregationType in AGGREGATE_EVENTS)) {
    return new Error("Invalid aggregation provided");
  }

  if (!VALID_TIME_UNIT[timeUnit]) {
    return new Error("Invalid time unit provided");
  }

  let previous;
  let filters;
  let eventName;

  if (options) {
    previous = options.previous;
    filters = options.filters;
    eventName = options.eventName;
  }

  let dateRangeStart;

  if (!previous) {
    previous = 0;
  }

  dateRangeStart = ADJUST_DATE[timeUnit as keyof AdjustDate](
    previous,
    new Date(),
  );

  const result = await dbQuery(
    AGGREGATE_EVENTS[aggregationType](timeUnit),
    dateRangeStart?.toISOString(),
    eventName,
    filters ? JSON.stringify(filters.events) : "{}",
    filters ? JSON.stringify(filters.users) : "{}",
  );

  if (result instanceof Error) {
    return Error;
  }

  const records: QueryResultRow[] = result.rows;
  return formatDataBy(timeUnit, records, previous);
}

async function getAllEventNames() {
  const result = await dbQuery(events.getAllEventNames());

  if (result instanceof Error) {
    return result;
  }

  return result.rows;
}

async function getAllAttributes() {
  const eventAttributesPromise = dbQuery(events.getAllEventAttributes());
  const userAttributesPromise = dbQuery(users.getAllUserAttributes());

  const [eventAttributesResult, userAttributesResult] = await Promise.all([
    eventAttributesPromise,
    userAttributesPromise,
  ]);

  if (eventAttributesResult instanceof Error) {
    return eventAttributesResult;
  }

  if (userAttributesResult instanceof Error) {
    return userAttributesResult;
  }

  const formattedResult = formatAttributes([
    eventAttributesResult,
    userAttributesResult,
  ]);

  return formattedResult;
}

export default {
  getAllEventNames,
  getAggregatedEventsBy,
  getAggregatedUsersBy,
  getAllAttributes,
};
