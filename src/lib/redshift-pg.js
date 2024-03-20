"use strict";

const dbQuery = require("../utils/db-query");

const events = {
  async listAll() {
    const result = await dbQuery(`SELECT DISTINCT event_name FROM events;`);
    return result.rows;
  },
  async getTotalByHour({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
        COUNT(DISTINCT event_id) AS event_count
      FROM
        events
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
        AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      GROUP BY
        event_hour`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getTotalByDay({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
        COUNT(DISTINCT event_id) AS event_count
      FROM
        events
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
        AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      GROUP BY
        event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getTotalByMonth({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM') AS event_month,
        COUNT(DISTINCT event_id) AS event_count
      FROM
        events
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
        AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      GROUP BY
        event_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getAveragePerUserByHour({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_hour AS (
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
       event_hour`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getAveragePerUserByDay({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_day AS (
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
       event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getAveragePerUserByMonth({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_month AS (
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
       event_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMinPerUserByHour({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_hour AS (
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
       event_hour`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMinPerUserByDay({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_day AS (
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
       event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMinPerUserByMonth({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_month AS (
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
       event_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMaxPerUserByHour({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_hour AS (
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
       event_hour`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMaxPerUserByDay({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_day AS (
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
       event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMaxPerUserByMonth({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_month AS (
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
       event_month`,

      dateRangeStart,
      event_name,
    );

    return result.rows[-1].max_event_count_per_user;
  },

  async getMedianPerUserByHour({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_hour AS (
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
     GROUP BY event_hour`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMedianPerUserByDay({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_day AS (
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
       event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getMedianPerUserByMonth({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      `WITH events_per_user_per_month AS (
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
       event_month`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },
};

const users = {
  async getTotalByHour({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      `SELECT
         TO_CHAR(event_created, 'YYYY-MM-DD HH24:00') AS event_hour,
         COUNT(DISTINCT user_id) AS user_count
      FROM
        events
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
        AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      GROUP BY
        event_hour`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getTotalByDay({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM-DD') AS event_day,
        COUNT(DISTINCT user_id) AS user_count
      FROM
        events
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
        AND CAST($2 AS VARCHAR) IS NULL OR event_name = $2
      GROUP BY
        event_day`,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },

  async getTotalByMonth({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      `SELECT
        TO_CHAR(event_created, 'YYYY-MM') AS event_month,
        COUNT(DISTINCT user_id) AS user_count
      FROM
        events
      WHERE
        (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
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
/* events.getTotalByDay({ previous: 30 }).then((val) => console.log(val)); */

// events
//   .getTotalByMonth({ previous: 1, event_name: "Login" })
//   .then((val) => console.log(val));

// events.getMaxPerUserByDay({ previous: 30 }).then((val) => console.log(val));

// events
//   .getAveragePerUserByMonth({ previous: 48 })
//   .then((val) => console.log(val));
//
// users.getTotalByDay({ previous: 1 }).then((val) => console.log(val));

// users
//   .getTotalByMonth({ previous: 1, event_name: "Login" })
//   .then((val) => console.log(val));
