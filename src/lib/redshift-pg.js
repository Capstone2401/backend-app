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
