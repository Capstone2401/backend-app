"use strict";

const dbQuery = require("../utils/db-query");

const events = {
  async listAll() {
    const result = await dbQuery(`SELECT DISTINCT event_name FROM events;`);
    return result.rows;
  },
  async getTotalByDay({ previousDays, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setDate(dateRangeStart.getDate() - previousDays || 0);

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
        COUNT(DISTINCT event_id) AS event_count
      FROM
        events
      WHERE
        CAST($1 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE
        AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      GROUP BY
        event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getTotalByMonth({ previousMonths, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setMonth(dateRangeStart.getMonth() - previousMonths || 0);

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM') AS event_month,
        COUNT(DISTINCT event_id) AS event_count
      FROM
        events
      WHERE
        CAST($1 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE
        AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      GROUP BY
        event_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getAveragePerUserByDay({ previousDays, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setDate(dateRangeStart.getDate() - previousDays || -1);

    const result = await dbQuery(
      `WITH events_per_user_per_day AS (
       SELECT
         TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
         COUNT(DISTINCT event_id) AS event_count,
         user_id
       FROM
         events
       WHERE
         (CAST($0 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
         AND (CAST($1 AS VARCHAR) IS NULL OR event_name = $2)
       GROUP BY
         event_day, user_id
     )
     SELECT
       event_day,
       AVG(CAST(event_count AS FLOAT)) AS average_event_count_per_user
     FROM
       events_per_user_per_day
     GROUP BY
       event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getAveragePerUserByMonth({ previousMonths, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setMonth(dateRangeStart.getMonth() - (previousMonths || -1));

    const result = await dbQuery(
      `WITH events_per_user_per_month AS (
       SELECT
         TO_CHAR(event_created, 'YYYY-MM') AS event_month,
         COUNT(DISTINCT event_id) AS event_count,
         user_id
       FROM
         events
       WHERE
         (CAST($0 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
         AND (CAST($1 AS VARCHAR) IS NULL OR event_name = $2)
       GROUP BY
         event_month, user_id
     )
     SELECT
       event_month,
       AVG(CAST(event_count AS FLOAT)) AS average_event_count_per_user
     FROM
       events_per_user_per_month
     GROUP BY
       event_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMinPerUserByDay({ previousDays, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setDate(dateRangeStart.getDate() - (previousDays || -1));

    const result = await dbQuery(
      `WITH events_per_user_per_day AS (
       SELECT
         TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
         COUNT(DISTINCT event_id) AS event_count,
         user_id
       FROM
         events
       WHERE
         (CAST($0 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
         AND (CAST($1 AS VARCHAR) IS NULL OR event_name = $2)
       GROUP BY
         event_day, user_id
     )
     SELECT
       event_day,
       MIN(event_count) AS min_event_count_per_user
     FROM
       events_per_user_per_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows[-1].min_event_count_per_user;
  },

  async getMinPerUserByMonth({ previousMonths, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setMonth(dateRangeStart.getMonth() - (previousMonths || -1));

    const result = await dbQuery(
      `WITH events_per_user_per_month AS (
       SELECT
         TO_CHAR(event_created, 'YYYY-MM') AS event_month,
         COUNT(DISTINCT event_id) AS event_count,
         user_id
       FROM
         events
       WHERE
         (CAST($0 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
         AND (CAST($1 AS VARCHAR) IS NULL OR event_name = $2)
       GROUP BY
         event_month, user_id
     )
     SELECT
       event_month,
       MIN(event_count) AS min_event_count_per_user
     FROM
       events_per_user_per_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows[-1].min_event_count_per_user;
  },

  async getMaxPerUserByDay({ previousDays, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setDate(dateRangeStart.getDate() - (previousDays || -1));

    const result = await dbQuery(
      `WITH events_per_user_per_day AS (
       SELECT
         TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
         COUNT(DISTINCT event_id) AS event_count,
         user_id
       FROM
         events
       WHERE
         (CAST($0 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
         AND (CAST($1 AS VARCHAR) IS NULL OR event_name = $2)
       GROUP BY
         event_day, user_id
     )
     SELECT
       event_day,
       MAX(event_count) AS max_event_count_per_user
     FROM
       events_per_user_per_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows[-1].max_event_count_per_user;
  },

  async getMaxPerUserByMonth({ previousMonths, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setMonth(dateRangeStart.getMonth() - (previousMonths || -1));

    const result = await dbQuery(
      `WITH events_per_user_per_month AS (
       SELECT
         TO_CHAR(event_created, 'YYYY-MM') AS event_month,
         COUNT(DISTINCT event_id) AS event_count,
         user_id
       FROM
         events
       WHERE
         (CAST($0 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
         AND (CAST($1 AS VARCHAR) IS NULL OR event_name = $2)
       GROUP BY
         event_month, user_id
     )
     SELECT
       event_month,
       MAX(event_count) AS max_event_count_per_user
     FROM
       events_per_user_per_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows[-1].max_event_count_per_user;
  },

  async getMedianPerUserByDay({ previousDays, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setDate(dateRangeStart.getDate() - (previousDays || -1));

    const result = await dbQuery(
      `WITH events_per_user_per_day AS (
       SELECT
         TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
         COUNT(DISTINCT event_id) AS event_count,
         user_id
       FROM
         events
       WHERE
         (CAST($0 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
         AND (CAST($1 AS VARCHAR) IS NULL OR event_name = $2)
       GROUP BY
         event_day, user_id
     )
     SELECT
       event_day,
       MEDIAN(event_count) AS median_event_count_per_user
     FROM
       events_per_user_per_day
     GROUP BY
       event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMedianPerUserByMonth({ previousMonths, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setMonth(dateRangeStart.getMonth() - (previousMonths || -1));

    const result = await dbQuery(
      `WITH events_per_user_per_month AS (
       SELECT
         TO_CHAR(event_created, 'YYYY-MM') AS event_month,
         COUNT(DISTINCT event_id) AS event_count,
         user_id
       FROM
         events
       WHERE
         (CAST($0 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
         AND (CAST($1 AS VARCHAR) IS NULL OR event_name = $2)
       GROUP BY
         event_month, user_id
     )
     SELECT
       event_month,
       MEDIAN(event_count) AS median_event_count_per_user
     FROM
       events_per_user_per_month
     GROUP BY
       event_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },
};

const users = {
  async getTotalByDay({ previousDays, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setDate(dateRangeStart.getDate() - previousDays || 0);

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
        COUNT(DISTINCT user_id) AS user_count
      FROM
        events
      WHERE
        CAST($1 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE
        AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      GROUP BY
        event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getTotalByMonth({ previousMonths, event_name }) {
    let dateRangeStart = new Date();
    dateRangeStart.setMonth(dateRangeStart.getMonth() - previousMonths || 0);

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM') AS event_month,
        COUNT(DISTINCT user_id) AS user_count
      FROM
        events
      WHERE
        CAST($1 AS DATE) IS NULL OR event_created BETWEEN $1 AND SYSDATE
        AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      GROUP BY
        event_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },
};

// events.listAll().then((val) => console.log(val));
// events
//   .getTotalbyDay({ previousDays: 30, event_name: "Login" })
//   .then((val) => console.log(val));
//
// events
//   .getTotalByMonth({ previousMonths: 1, event_name: "Login" })
//   .then((val) => console.log(val));
//
events.getMaxPerUserByDay({ previousDays: 30 }).then((val) => console.log(val));

// users
//   .getTotalByDay({ previousDays: 1, event_name: "Login" })
//   .then((val) => console.log(val));
//
// users
//   .getTotalByMonth({ previousMonths: 1, event_name: "Login" })
//   .then((val) => console.log(val));
