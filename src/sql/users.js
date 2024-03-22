module.exports = {
  totalByHour: `
    SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
       COUNT(DISTINCT user_id) AS user_count
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      event_hour
  `,

  totalByDay: `
    SELECT
      TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
      COUNT(DISTINCT user_id) AS user_count
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      event_day
  `,

  totalByMonth: `
    SELECT
      TO_CHAR(event_created, 'YYYY-MM') AS event_month,
      COUNT(DISTINCT user_id) AS user_count
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      event_month
  `,
};
