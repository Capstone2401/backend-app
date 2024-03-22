module.exports = {
  listAll: `
    SELECT DISTINCT event_name FROM events;
  `,

  listAllAttributes: `
    SELECT DISTINCT event_attributes from events;
  `,

  totalByHour: `
    SELECT
      DATE_TRUNC('hour', event_created) AS event_hour,
      COUNT(DISTINCT event_id) AS calculated_value
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      event_hour;
  `,

  totalByDay: `
    SELECT
      DATE_TRUNC('day', event_created) AS event_day,
      COUNT(DISTINCT event_id) AS calculated_value
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      event_day;

  `,

  totalByMonth: `
    SELECT
      DATE_TRUNC('month', event_created) AS event_month,
      COUNT(DISTINCT event_id) AS calculated_value
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      event_month;

  `,

  averagePerUserByHour: `
    WITH events_per_user_per_hour AS (
        SELECT
            TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
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
            TO_CHAR(e.event_created, 'YYYY-MM-DD HH24:00')
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
          event_day

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
    WITH events_per_user_per_day AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD hh24') AS event_day,
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
WITH event_counts AS (
      SELECT
        user_id,
        TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
        COUNT(DISTINCT e.event_id) as num_events
      FROM events e
            WHERE
              (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
              AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
              AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(e.event_attributes), $3) = $4)
     GROUP BY 
       user_id,
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00')

    ), 
  user_event_counts AS (
       SELECT
            u.user_id,
            u.user_created,
            COALESCE(ec.event_hour, (SELECT event_hour FROM event_counts WHERE event_hour IS NOT NULL LIMIT 1)) AS event_hour,
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
      event_hour;`,

  medianPerUserByDay: `
WITH event_counts AS (
      SELECT
        user_id,
        TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
        COUNT(DISTINCT e.event_id) as num_events
      FROM events e
            WHERE
              (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
              AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
              AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(e.event_attributes), $3) = $4)
     GROUP BY 
       user_id,
       TO_CHAR(event_created, 'YYYY-MM-DD')

    ), 
  user_event_counts AS (
       SELECT
            u.user_id,
            u.user_created,
            COALESCE(ec.event_day, (SELECT event_day FROM event_counts WHERE event_day IS NOT NULL LIMIT 1)) AS event_day,
            CAST(COALESCE(ec.num_events, 0) AS INT) AS num_events
        FROM
           users u
        LEFT JOIN event_counts ec ON
            u.user_id = ec.user_id
    )

    SELECT
        event_day,
        CAST(MEDIAN(num_events) AS FLOAT) AS calculated_value
    FROM
        user_event_counts
    WHERE user_created < event_day
    GROUP BY
      event_day;
  `,

  medianPerUserByMonth: `
WITH event_counts AS (
      SELECT
        user_id,
        TO_CHAR(event_created, 'YYYY-MM') AS event_month,
        COUNT(DISTINCT e.event_id) as num_events
      FROM events e
            WHERE
              (CAST($1 AS TIMESTAMP) IS NULL OR e.event_created BETWEEN $1 AND SYSDATE)
              AND (CAST($2 AS VARCHAR) IS NULL OR e.event_name = $2)
              AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(e.event_attributes), $3) = $4)
     GROUP BY 
       user_id,
       TO_CHAR(event_created, 'YYYY-MM')

    ), 
  user_event_counts AS (
       SELECT
            u.user_id,
            u.user_created,
            COALESCE(ec.event_month, (SELECT event_month FROM event_counts WHERE event_month IS NOT NULL LIMIT 1)) AS event_month,
            CAST(COALESCE(ec.num_events, 0) AS INT) AS num_events
        FROM
           users u
        LEFT JOIN event_counts ec ON
            u.user_id = ec.user_id
    )

    SELECT
        event_month,
        CAST(MEDIAN(num_events) AS FLOAT) AS calculated_value
    FROM
        user_event_counts
    WHERE user_created < event_month
    GROUP BY
      event_month;
  `,
};
