"use strict";

const dbQuery = require("../utils/db-query");
const eventsSQL = require("../sql/events.js");
const usersSQL = require("../sql/users.js");

const events = {
  async listAll() {
    const result = await dbQuery(eventsSQL.listAll);
    return result.rows;
  },
  async getTotalByHour({ previous, event_name }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      eventsSQL.totalByHour,
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
      eventsSQL.totalByDay,
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
      eventsSQL.totalByMonth,
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
      eventsSQL.averagePerUserByHour,
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
      eventsSQL.averagePerUserByDay,
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
      eventsSQL.averagePerUserByMonth,
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
      eventsSQL.minPerUserByHour,
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
      eventsSQL.minPerUserByDay,
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
      eventsSQL.minPerUserByMonth,
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
      eventsSQL.minPerUserByHour,
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
      eventsSQL.maxPerUserByDay,
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
      eventsSQL.maxPerUserByMonth,
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
      events.getMedianPerUserByHour,
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
      eventsSQL.medianPerUserByDay,
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
      eventsSQL.medianPerUserByMonth,
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
      usersSQL.totalByHour,
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
      usersSQL.totalByDay,
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
      usersSQL.totalByMonth,
      dateRangeStart,
      event_name,
    );

    return result.rows;
  },
};

module.exports = {
  events,
  users,
};

// events.listAll().then((val) => console.log(val));
// events.getTotalByDay({ previous: 30 }).then((val) => console.log(val));

// events
//   .getTotalByMonth({ previous: 1, event_name: "Login" })
//   .then((val) => console.log(val));
//
// events.getMaxPerUserByDay({ previous: 30 }).then((val) => console.log(val));
//
// events
//   .getAveragePerUserByMonth({ previous: 48 })
//   .then((val) => console.log(val));
// //
// users.getTotalByDay({ previous: 1 }).then((val) => console.log(val));
//
// users
//   .getTotalByMonth({ previous: 1, event_name: "Login" })
//   .then((val) => console.log(val));
