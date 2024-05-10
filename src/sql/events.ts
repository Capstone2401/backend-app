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
    WITH events_per_user_per_${timeUnit} AS (
      SELECT
          e.user_id,
          DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
          COALESCE(COUNT(*), 0) AS count
      FROM
          events e
      LEFT JOIN
      users u ON e.event_created = u.user_created
      WHERE
          (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
          AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
          AND (CAST($3 AS VARCHAR) IS NULL OR attr_validate($3, JSON_SERIALIZE(e.event_attributes)))
          AND (CAST($4 AS VARCHAR) IS NULL OR attr_validate($4, JSON_SERIALIZE(u.user_attributes)))
      GROUP BY
          e.user_id, ${timeUnit}
  ),
  medians_per_${timeUnit} AS (
      SELECT
          ${timeUnit},
          MEDIAN(count) AS median_count
      FROM
          events_per_user_per_${timeUnit}
      GROUP BY
          ${timeUnit}
    )
    SELECT
        ${timeUnit},
        median_count AS calculated_value
    FROM
        medians_per_${timeUnit};  `;
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
