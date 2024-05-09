import { VALID_TIME_UNIT } from "src/lib/globals";
import { ResponseError } from "src/utils/response-error";
import { TimeUnit } from "src/types/time";

function getAllEventNames(): string {
  return `
    SELECT
      DISTINCT event_name
    FROM
      events
  `;
}

function getAllEventAttributes(): string {
  return `
  SELECT 
    DISTINCT
      JSON_SERIALIZE(event_attributes)
    FROM
      events
  `;
}

function getTotalEventsBy(timeUnit: TimeUnit): string {
  if (!VALID_TIME_UNIT[timeUnit]) {
    throw new ResponseError({
      message: "Invalid time unit provided",
      statusCode: 400,
    });
  }

  return `
    SELECT
      DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
      COUNT(DISTINCT e.id) AS calculated_value
    FROM
      events e
    LEFT JOIN
      users u ON u.user_id = e.user_id
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR attr_validate($3, JSON_SERIALIZE(e.event_attributes)))
      AND (CAST($4 AS VARCHAR) IS NULL OR attr_validate($4, JSON_SERIALIZE(u.user_attributes)))
    GROUP BY
      ${timeUnit}
  `;
}

function getAveragePerUserBy(timeUnit: TimeUnit): string {
  if (!VALID_TIME_UNIT[timeUnit]) {
    throw new ResponseError({
      message: "Invalid time unit provided",
      statusCode: 400,
    });
  }

  return `
    WITH events_per_user_per_${timeUnit} AS (
        SELECT
            DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
            COUNT(DISTINCT e.id) AS calculated_value,
            COUNT(DISTINCT u.user_id) AS user_count
        FROM
            events e
        LEFT JOIN
            users u ON u.user_created <= e.event_created
        WHERE
            (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
            AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
            AND (CAST($3 AS VARCHAR) IS NULL OR attr_validate($3, JSON_SERIALIZE(e.event_attributes)))
            AND (CAST($4 AS VARCHAR) IS NULL OR attr_validate($4, JSON_SERIALIZE(u.user_attributes)))
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

function getMinPerUserBy(timeUnit: TimeUnit): string {
  if (!VALID_TIME_UNIT[timeUnit]) {
    throw new ResponseError({
      message: "Invalid time unit provided",
      statusCode: 400,
    });
  }

  return `
    WITH events_per_user_per_${timeUnit} AS (
    SELECT
      DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
      COUNT(DISTINCT e.id) AS calculated_value,
      e.user_id
    FROM
      events e
    LEFT JOIN
      users u ON u.user_id = e.user_id
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR attr_validate($3, JSON_SERIALIZE(e.event_attributes)))
      AND (CAST($4 AS VARCHAR) IS NULL OR attr_validate($4, JSON_SERIALIZE(u.user_attributes)))
    GROUP BY
      ${timeUnit}, e.user_id
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

function getMaxPerUserBy(timeUnit: TimeUnit): string {
  if (!VALID_TIME_UNIT[timeUnit]) {
    throw new ResponseError({
      message: "Invalid time unit provided",
      statusCode: 400,
    });
  }

  return `
    WITH events_per_user_per_${timeUnit} AS (
    SELECT
      DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
      COUNT(DISTINCT e.id) AS calculated_value,
      e.user_id
    FROM
      events e
    LEFT JOIN
      users u ON u.user_id = e.user_id
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR attr_validate($3, JSON_SERIALIZE(e.event_attributes)))
      AND (CAST($4 AS VARCHAR) IS NULL OR attr_validate($4, JSON_SERIALIZE(u.user_attributes)))
    GROUP BY
      ${timeUnit}, e.user_id
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

function getMedianPerUserBy(timeUnit: TimeUnit): string {
  if (!VALID_TIME_UNIT[timeUnit]) {
    throw new ResponseError({
      message: "Invalid time unit provided",
      statusCode: 400,
    });
  }

  return `
    WITH event_counts AS (
      SELECT
        e.user_id,
        DATE_TRUNC('hour', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
        COUNT(DISTINCT e.id) as num_events
      FROM events e
      LEFT JOIN
        users u ON u.user_id = e.user_id
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
        AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
        AND (CAST($3 AS VARCHAR) IS NULL OR attr_validate($3, JSON_SERIALIZE(e.event_attributes)))
        AND (CAST($4 AS VARCHAR) IS NULL OR attr_validate($4, JSON_SERIALIZE(u.user_attributes)))
      GROUP BY
        e.user_id,
        ${timeUnit}
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

export default {
  getAllEventNames,
  getTotalEventsBy,
  getAveragePerUserBy,
  getMaxPerUserBy,
  getMinPerUserBy,
  getMedianPerUserBy,
  getAllEventAttributes,
};
