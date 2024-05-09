import dbQuery from "src/utils/db-query";
import events from "src/sql/events";
import users from "src/sql/users";
import { VALID_TIME_UNIT } from "src/lib/globals";
import formatDataBy from "src/utils/format-records";
import formatAttributes from "src/utils/format-attributes";

import { QueryResultRow } from "pg";
import { QueryArgs } from "src/types/query";
import { AggregateEvents, AggregateUsers } from "src/types/redshift";
import { DateMap, DateOffsetMethod } from "src/types/time";
import { ResponseError } from "src/utils/response-error";
import { FormattedAttributes } from "src/types/format";

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
  week: (offset, currDate) => {
    currDate.setDate(currDate.getDate() - offset * 7);
    return currDate;
  },
};

async function getAggregatedUsersBy({
  timeUnit,
  aggregationType,
  options,
}: QueryArgs) {
  if (!(aggregationType in AGGREGATE_USERS)) {
    throw new ResponseError({
      message: "Invalid aggregation provided " + aggregationType,
      statusCode: 400,
    });
  }

  if (!VALID_TIME_UNIT[timeUnit]) {
    throw new ResponseError({
      message: "Invalid time unit provided " + timeUnit,
      statusCode: 400,
    });
  }

  let { previous, filters, eventName } = options ?? {
    previous: null,
    filters: null,
    eventName: null,
  };

  let dateRangeStart;
  previous = previous ?? 0;

  dateRangeStart = ADJUST_DATE[timeUnit](previous, new Date());

  const result = await dbQuery(
    AGGREGATE_USERS[aggregationType as keyof AggregateUsers](timeUnit),
    dateRangeStart?.toISOString(),
    eventName,
    filters ? JSON.stringify(filters.events) : "{}",
    filters ? JSON.stringify(filters.users) : "{}",
  );

  const records: QueryResultRow[] = result.rows;
  return formatDataBy(timeUnit, aggregationType, records, previous);
}

async function getAggregatedEventsBy({
  timeUnit,
  aggregationType,
  options,
}: QueryArgs) {
  if (!(aggregationType in AGGREGATE_EVENTS)) {
    throw new ResponseError({
      message: "Invalid aggregation provided " + aggregationType,
      statusCode: 400,
    });
  }

  if (!VALID_TIME_UNIT[timeUnit]) {
    throw new ResponseError({
      message: "Invalid time unit provided " + timeUnit,
      statusCode: 400,
    });
  }

  let { previous, filters, eventName } = options ?? {
    previous: null,
    filters: null,
    eventName: null,
  };

  let dateRangeStart;
  previous = previous ?? 0;

  dateRangeStart = ADJUST_DATE[timeUnit as keyof AdjustDate](
    previous,
    new Date(),
  );

  const result = await dbQuery(
    AGGREGATE_EVENTS[aggregationType as keyof AggregateEvents](timeUnit),
    dateRangeStart?.toISOString(),
    eventName,
    filters ? JSON.stringify(filters.events) : "{}",
    filters ? JSON.stringify(filters.users) : "{}",
  );

  const records: QueryResultRow[] = result.rows;
  return formatDataBy(timeUnit, aggregationType, records, previous);
}

async function getAllEventNames(): Promise<QueryResultRow[]> {
  const result = await dbQuery(events.getAllEventNames());
  return result.rows;
}

async function getAllAttributes(): Promise<FormattedAttributes> {
  const eventAttributesPromise = dbQuery(events.getAllEventAttributes());
  const userAttributesPromise = dbQuery(users.getAllUserAttributes());

  const [eventAttributesResult, userAttributesResult] = await Promise.all([
    eventAttributesPromise,
    userAttributesPromise,
  ]);

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
