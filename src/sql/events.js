module.exports = {
  listAll: `
    SELECT DISTINCT event_name FROM events;
  `,

  listAllAttributes: `
    SELECT DISTINCT event_attributes from events;
  `,

  totalByHour: `
    SELECT
      TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
      COUNT(DISTINCT event_id) AS calculated_value
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
      COUNT(DISTINCT event_id) AS calculated_value
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
      COUNT(DISTINCT event_id) AS calculated_value
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      event_month
  `,

  averagePerUserByHour: `
    WITH events_per_user_per_hour AS (
        SELECT
            TO_CHAR(event_created, 'YYYY-MM-DD HH24') AS event_hour,
            COUNT(DISTINCT event_id) AS calculated_value,
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
            TO_CHAR(e.event_created, 'YYYY-MM-DD HH24')
    )
    SELECT
        event_hour,
        CAST(SUM(calculated_value) AS FLOAT) / NULLIF(SUM(user_count), 0) AS calculated_value
    FROM
        events_per_user_per_hour
    GROUP BY
        event_hour;
  `,

  averagePerUserByDay: `
    WITH events_per_user_per_day AS (
      SELECT
          TO_CHAR(e.event_created, 'YYYY-MM-DD') AS event_day,
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
          TO_CHAR(e.event_created, 'YYYY-MM-DD')

   )
  
   SELECT
     event_day,
     CAST(SUM(calculated_value) AS FLOAT) / NULLIF(user_count, 0) AS calculated_value
   FROM
     events_per_user_per_day
   GROUP BY
     event_day, user_count
  `,

  averagePerUserByMonth: `
    WITH events_per_user_per_month AS (
        SELECT
            TO_CHAR(event_created, 'YYYY-MM') AS event_month,
            COUNT(DISTINCT event_id) AS calculated_value,
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
            TO_CHAR(e.event_created, 'YYYY-MM')
    )
    SELECT
        event_month,
        CAST(SUM(calculated_value) AS FLOAT) / NULLIF(SUM(user_count), 0) AS calculated_value
    FROM
        events_per_user_per_month
    GROUP BY
        event_month;  `,

  minPerUserByHour: `
    WITH events_per_user_per_hour AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_hour, user_id
   )
   SELECT
     event_hour,
     MIN(calculated_value) AS calculated_value
   FROM
     events_per_user_per_hour
   GROUP BY
     event_hour
  `,

  minPerUserByDay: `
    WITH events_per_user_per_day AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_day, user_id
   )
   SELECT
     event_day,
     MIN(calculated_value) AS calculated_value
   FROM
     events_per_user_per_day
   GROUP BY
     event_day
  `,

  minPerUserByMonth: `
    WITH events_per_user_per_month AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM') AS event_month,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_month, user_id
   )
   SELECT
     event_month,
     MIN(calculated_value) AS calculated_value
   FROM
     events_per_user_per_month
   GROUP BY
     event_month
  `,

  maxPerUserByHour: `
    WITH events_per_user_per_hour AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_hour, user_id
   )
   SELECT
     event_hour,
     MAX(calculated_value) AS calculated_value
   FROM
     events_per_user_per_hour
   GROUP BY
     event_hour
  `,

  maxPerUserByDay: `
    WITH events_per_user_per_day AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_day, user_id
   )
   SELECT
     event_day,
     MAX(calculated_value) AS calculated_value
   FROM
     events_per_user_per_day
   GROUP BY
     event_day
  `,

  maxPerUserByMonth: `
    WITH events_per_user_per_month AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM') AS event_month,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_month, user_id
   )
   SELECT
     event_month,
     MAX(calculated_value) AS calculated_value
   FROM
     events_per_user_per_month
   GROUP BY
     event_month
  `,

  medianPerUserByHour: `
    WITH events_per_user_per_hour AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_hour, user_id
   )
   SELECT
     event_hour,
     MEDIAN(calculated_value) AS calculated_value
   FROM
     events_per_user_per_hour
   GROUP BY event_hour
  `,

  medianPerUserByDay: `
    WITH events_per_user_per_day AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_day, user_id
   )
   SELECT
     event_day,
     MEDIAN(calculated_value) AS calculated_value
   FROM
     events_per_user_per_day
   GROUP BY
     event_day
  `,

  medianPerUserByMonth: `
    WITH events_per_user_per_month AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM') AS event_month,
       COUNT(DISTINCT event_id) AS calculated_value,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
       AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
     GROUP BY
       event_month, user_id
   )
   SELECT
     event_month,
     MEDIAN(calculated_value) AS calculated_value
   FROM
     events_per_user_per_month
   GROUP BY
     event_month
  `,
};
