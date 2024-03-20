module.exports = {
  listAll: `
    SELECT DISTINCT event_name FROM events;
  `,

  totalByHour: `
    SELECT
      TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
      COUNT(DISTINCT event_id) AS event_count
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
    GROUP BY
      event_hour
  `,

  totalByDay: `
    SELECT
      TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
      COUNT(DISTINCT event_id) AS event_count
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
    GROUP BY
      event_day
  `,

  totalByMonth: `
    SELECT
      TO_CHAR(event_created, 'YYYY-MM') AS event_month,
      COUNT(DISTINCT event_id) AS event_count
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
    GROUP BY
      event_month
  `,

  averagePerUserByHour: `
    WITH events_per_user_per_hour AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_hour, user_id
   )
   SELECT
     event_hour,
     AVG(CAST(event_count AS FLOAT)) AS average_event_count_per_user
   FROM
     events_per_user_per_hour
   GROUP BY
     event_hour
  `,

  averagePerUserByDay: `
    WITH events_per_user_per_day AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_day, user_id
   )
   SELECT
     event_day,
     AVG(CAST(event_count AS FLOAT)) AS average_event_count_per_user
   FROM
     events_per_user_per_day
   GROUP BY
     event_day
  `,

  averagePerUserByMonth: `
    WITH events_per_user_per_month AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM') AS event_month,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_month, user_id
   )
   SELECT
     event_month,
     AVG(CAST(event_count AS FLOAT)) AS average_event_count_per_user
   FROM
     events_per_user_per_month
   GROUP BY
     event_month
  `,

  minPerUserByHour: `
    WITH events_per_user_per_hour AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_hour, user_id
   )
   SELECT
     event_hour,
     MIN(event_count) AS min_event_count_per_user
   FROM
     events_per_user_per_hour
   GROUP BY
     event_hour
  `,

  minPerUserByDay: `
    WITH events_per_user_per_day AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_day, user_id
   )
   SELECT
     event_day,
     MIN(event_count) AS min_event_count_per_user
   FROM
     events_per_user_per_day
   GROUP BY
     event_day
  `,

  minPerUserByMonth: `
    WITH events_per_user_per_month AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM') AS event_month,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_month, user_id
   )
   SELECT
     event_month,
     MIN(event_count) AS min_event_count_per_user
   FROM
     events_per_user_per_month
   GROUP BY
     event_month
  `,

  maxPerUserByHour: `
    WITH events_per_user_per_hour AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_hour, user_id
   )
   SELECT
     event_hour,
     MAX(event_count) AS max_event_count_per_user
   FROM
     events_per_user_per_hour
   GROUP BY
     event_hour
  `,

  maxPerUserByDay: `
    WITH events_per_user_per_day AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_day, user_id
   )
   SELECT
     event_day,
     MAX(event_count) AS max_event_count_per_user
   FROM
     events_per_user_per_day
   GROUP BY
     event_day
  `,

  maxPerUserByMonth: `
    WITH events_per_user_per_month AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM') AS event_month,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_month, user_id
   )
   SELECT
     event_month,
     MAX(event_count) AS max_event_count_per_user
   FROM
     events_per_user_per_month
   GROUP BY
     event_month
  `,

  medianPerUserByHour: `
    WITH events_per_user_per_hour AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_hour, user_id
   )
   SELECT
     event_hour,
     MEDIAN(event_count) AS median_event_count_per_user
   FROM
     events_per_user_per_hour
   GROUP BY event_hour
  `,

  medianPerUserByDay: `
    WITH events_per_user_per_day AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_day, user_id
   )
   SELECT
     event_day,
     MEDIAN(event_count) AS median_event_count_per_user
   FROM
     events_per_user_per_day
   GROUP BY
     event_day
  `,

  medianPerUserByMonth: `
    WITH events_per_user_per_month AS (
     SELECT
       TO_CHAR(event_created, 'YYYY-MM') AS event_month,
       COUNT(DISTINCT event_id) AS event_count,
       user_id
     FROM
       events
     WHERE
       (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
       AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
     GROUP BY
       event_month, user_id
   )
   SELECT
     event_month,
     MEDIAN(event_count) AS median_event_count_per_user
   FROM
     events_per_user_per_month
   GROUP BY
     event_month
  `,
};
