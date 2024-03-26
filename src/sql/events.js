const { VALID_TIME_UNIT } = require("../lib/globals");

function getAllEventNames() {
  return `
    SELECT
      DISTINCT event_name
    FROM
      events
  `;
}

function getTotalEventsBy(timeUnit) {
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  return `
    SELECT
      DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
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
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  return `
    WITH events_per_user_per_${timeUnit} AS (
        SELECT
            DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
            COUNT(DISTINCT e.event_id) AS calculated_value,
            COUNT(DISTINCT u.user_id) AS user_count
        FROM
            events e
        LEFT JOIN
            users u ON u.user_created <= e.event_created
        WHERE
            (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
            AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
            AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(e.event_attributes), $3) = $4)
        GROUP BY
            ${timeUnit}
    )
    SELECT
        ${timeUnit},
        CAST(SUM(calculated_value) AS FLOAT) / NULLIF(SUM(user_count), 0) AS calculated_value
    FROM
        events_per_user_per_${timeUnit}
    GROUP BY
        ${timeUnit}
  `;
}

function getMinPerUserBy(timeUnit) {
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  return `
    WITH events_per_user_per_${timeUnit} AS (
    SELECT
      DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
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
    events_per_user_per_${timeUnit}
  GROUP BY
    ${timeUnit}
  `;
}

function getMaxPerUserBy(timeUnit) {
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  return `
    WITH events_per_user_per_${timeUnit} AS (
    SELECT
      DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
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
    events_per_user_per_${timeUnit}
  GROUP BY
    ${timeUnit}
  `;
}

function getMedianPerUserBy(timeUnit) {
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  return `
    WITH event_counts AS (
      SELECT
        user_id,
        DATE_TRUNC('hour', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
        COUNT(DISTINCT e.event_id) as num_events
      FROM events e
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
        AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
        AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(e.event_attributes), $3) = $4)
      GROUP BY
        user_id,
        DATE_TRUNC('hour', CAST(event_created AS TIMESTAMPTZ))
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
      ${timeUnit},
      CAST(MEDIAN(num_events) AS FLOAT) AS calculated_value
    FROM
      user_event_counts
    WHERE user_created <= ${timeUnit}
    GROUP BY
    ${timeUnit}
  `;
}

module.exports = {
  getTotalEventsBy,
  getAveragePerUserBy,
  getMaxPerUserBy,
  getMinPerUserBy,
  getMedianPerUserBy,
};
