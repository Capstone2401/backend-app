const dbQuery = require("./src/utils/db-query");

const VALID_DATE = {
  event_hour: "hour",
  event_day: "day", 
  event_month: "month",
};

// file: ./src/sql/users.js
function getTotalUsersBy(timeUnit) {
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";

  return `
    SELECT
      DATE_TRUNC('${VALID_DATE[timeUnit]}', event_created) AS ${timeUnit},
      COUNT(DISTINCT user_id) AS user_count
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      ${timeUnit}
  `;
}

// file: ./src/sql/events.js
function getTotalEventsBy(timeUnit) {
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";

  return `
    SELECT
      DATE_TRUNC('${VALID_DATE[timeUnit]}', event_created) AS ${timeUnit},
      COUNT(DISTINCT event_id) AS calculated_value
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      ${timeUnit}
  `;
}

function getAveragePerUserBy(timeUnit) {
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";

  return `
    WITH events_per_user_per_${VALID_DATE[timeUnit]} AS (
        SELECT
            DATE_TRUNC('${VALID_DATE[timeUnit]}', event_created) AS ${timeUnit},
            COUNT(DISTINCT e.event_id) AS calculated_value,
            COUNT(DISTINCT u.user_id) AS user_count
        FROM
            events e
        LEFT JOIN
            users u ON u.user_created < e.event_created
        WHERE
            (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
            AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
            AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(e.event_attributes), $3) = $4)
        GROUP BY
            DATE_TRUNC('${VALID_DATE[timeUnit]}', e.event_created)
    )
    SELECT
        ${timeUnit},
        CAST(SUM(calculated_value) AS FLOAT) / NULLIF(SUM(user_count), 0) AS calculated_value
    FROM
        events_per_user_per_${VALID_DATE[timeUnit]}
    GROUP BY
        ${timeUnit}
  `;
}

function getMinPerUserBy(timeUnit) {
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";

  return `
    WITH events_per_user_per_${VALID_DATE[timeUnit]} AS (
    SELECT
      DATE_TRUNC('${VALID_DATE[timeUnit]}', event_created) AS ${timeUnit},
      COUNT(DISTINCT event_id) AS calculated_value,
      user_id
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      ${timeUnit}, user_id
  )
  SELECT
    ${timeUnit},
    MIN(calculated_value) AS calculated_value
  FROM
    events_per_user_per_${VALID_DATE[timeUnit]}
  GROUP BY
    ${timeUnit}
  `;
}

function getMaxPerUserBy(timeUnit) {
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";

  return `
    WITH events_per_user_per_${VALID_DATE[timeUnit]} AS (
    SELECT
      DATE_TRUNC('${VALID_DATE[timeUnit]}', event_created) AS ${timeUnit},
      COUNT(DISTINCT event_id) AS calculated_value,
      user_id
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      ${timeUnit}, user_id
  )
  SELECT
    ${timeUnit},
    MAX(calculated_value) AS calculated_value
  FROM
    events_per_user_per_${VALID_DATE[timeUnit]}
  GROUP BY
    ${timeUnit}
  `;
}

function getMedianPerUserBy(timeUnit) {
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";

  return `
    WITH event_counts AS (
      SELECT
        user_id,
        DATE_TRUNC('hour', event_created) AS ${timeUnit},
        COUNT(DISTINCT e.event_id) as num_events
      FROM events e
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
        AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
        AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(e.event_attributes), $3) = $4)
      GROUP BY 
        user_id,
        DATE_TRUNC('hour', event_created)
    ), 
    user_event_counts AS (
      SELECT
        u.user_id,
        u.user_created,
        COALESCE(ec.${timeUnit}, (SELECT ${timeUnit} FROM event_counts WHERE ${timeUnit} IS NOT NULL LIMIT 1)) AS ${timeUnit},
        CAST(COALESCE(ec.num_events, 0) AS INT) AS num_events
      FROM
        users u
      LEFT JOIN event_counts ec ON
        u.user_id = ec.user_id
    )
    SELECT
      event_hour,
      CAST(MEDIAN(num_events) AS FLOAT) AS calculated_value
    FROM
      user_event_counts
    WHERE user_created < event_hour
    GROUP BY
    ${timeUnit}
  `
}

// ./src/utils/format-events.js and ./src/utils/format-users.js
// Suggested filename: .src/utils/format-records.js
const MS_IN_HOUR = 60 * 60 * 1000; // milliseconds
const MS_IN_DAY = 60 * 60 * 24 * 1000; // milliseconds

const GENERATE_DATE = {
  hour: (offset, currDate) => new Date(currDate.getTime() - offset * MS_IN_HOUR),
  day: (offset, currDate) => new Date(currDate.getTime() - offset * MS_IN_DAY),
  month: (offset, currDate) => new Date(currDate).setMonth(currDate.getMonth() - offset),
};

const FORMAT_DATE = {
  hour: (timestamp) => timestamp.toISOString().slice(0, 13) + ":00",
  day: (timestamp) => timestamp.toISOString().slice(0, 10),
  month: (timestamp) => {
    const year = timestamp.getFullYear();
    const month = (timestamp.getMonth() + 1).toString().padStart(2, 0);
    return `${year}-${month}`;
  },
};

function formatEventDataBy(timeUnit, records, previous) {
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";
  const timeKey = VALID_DATE[timeUnit];

  const currentDate = new Date();
  const pastNRecords = {};

  for (let idx = 0; idx < previous; idx++) {
    const timestamp = GENERATE_DATE[timeKey](idx, currentDate);
    const formatted = FORMAT_DATE[timeKey](timestamp);

    pastNRecords[formatted] = {
      event_hour: formatted,
      calculated_value: 0,
    };
  }

  for (const record of records) {
    const recordKey = FORMAT_DATE[timeKey](record.event_hour);
    if (pastNRecords[recordKey]) {
      pastNRecords[recordKey].calculated_value = Number(record.calculated_value);
    }
  }

  return Object.values(pastNRecords);
}

function formatUserDataBy(timeUnit, records, previous) {
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";
  const timeKey = VALID_DATE[timeUnit];

  const currentDate = new Date();
  const pastNRecords = {};

  for (let idx = 0; idx < previous; idx++) {
    const timestamp = GENERATE_DATE[timeKey](idx, currentDate);
    const formatted = FORMAT_DATE[timeKey](timestamp);

    pastNRecords[formatted] = {
      event_hour: formatted,
      user_count: 0,
    };
  }

  for (const record of records) {
    const recordKey = FORMAT_DATE[timeKey](record.event_hour);
    if (pastNRecords[recordKey]) {
      pastNRecords[recordKey].user_count = Number(record.user_count);
    }
  }

  return Object.values(pastNRecords);
}

// file: ./src/lib/redshift-pg.js
const ADJUST_DATE = {
  hour: (offset, currDate) => currDate.setHours(currDate.getHours() - offset),
  day: (offset, currDate) => currDate.setDate(currDate.getDate() - offset),
  month: (offset, currDate) => currDate.setMonth(currDate.getMonth() - offset),
};

const VALID_AGGREGATE_TYPES = {
  totalUsers: "totalUsers",
  totalEvents: "totalEvents",
  averageEvents: "averageEvents",
  minimumEvents: "minimumEvents",
  maximumEvents: "maximumEvents",
};

// functions would be imported from ./src/sql files
const AGGREGATE_ACTION = {
  totalUsers: getTotalUsersBy,
  totalEvents: getTotalEventsBy,
  averageEvents: getAveragePerUserBy,
  medianEvents: getMedianPerUserBy, 
  minimumEvents: getMinPerUserBy,
  maximumEvents: getMaxPerUserBy,
};


async function getAggregatedUsersBy(timeUnit, aggregationType, data) {
  if (!VALID_AGGREGATE_TYPES[aggregationType]) return "Invalid aggregation provided";
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";

  const timeKey = VALID_DATE[timeUnit];

  const { previous } = data;
  let dateRangeStart;

  if (previous) { 
    dateRangeStart = ADJUST_DATE[timeKey](previous, new Date());
  }

  const {
    event_name,
    filterAttribute,
    filterAttributeValue,
  } = data;

  const result = await dbQuery(
    AGGREGATE_ACTION[aggregationType](timeUnit),
    dateRangeStart,
    event_name,
    filterAttribute,
    filterAttributeValue,
  );

  return formatUserDataBy(timeUnit, result.rows, previous);
}

async function getAggregatedEventsBy(timeUnit, aggregationType, data) {
  if (!VALID_AGGREGATE_TYPES[aggregationType]) return "Invalid aggregation provided";
  if (!VALID_DATE[timeUnit]) return "Invalid time unit provided";
  
  const timeKey = VALID_DATE[timeUnit];

  const { previous } = data;
  let dateRangeStart;

  if (previous) { 
    dateRangeStart = ADJUST_DATE[timeKey](previous, new Date());
  }

  const {
    event_name,
    filterAttribute,
    filterAttributeValue,
  } = data;

  const result = await dbQuery(
    AGGREGATE_ACTION[aggregationType](timeUnit),
    dateRangeStart,
    event_name,
    filterAttribute,
    filterAttributeValue,
  );

  return formatEventDataBy(timeUnit, result.rows, previous);
}
