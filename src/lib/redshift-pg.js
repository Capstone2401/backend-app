"use strict";

const dbQuery = require("../utils/db-query");
const eventsSQL = require("../sql/events.js");
const usersSQL = require("../sql/users.js");
const formatEvents = require("../utils/format-events.js");
const formatUsers = require("../utils/format-users.js");

const events = {
  async listAll() {
    const result = await dbQuery(eventsSQL.listAll);
    return result.rows;
  },

  async listAllAttributes() {
    const transformedResult = {};
    const result = await dbQuery(eventsSQL.listAllAttributes);

    result.rows.forEach((record) => {
      const attributes = JSON.parse(record.event_attributes);

      for (const key in attributes) {
        transformedResult[key] = transformedResult[key] || [];
        transformedResult[key].push(attributes[key]);
      }
    });

    for (const attribute in transformedResult) {
      transformedResult[attribute] = Array.from(
        new Set(transformedResult[attribute]),
      );
    }

    return transformedResult;
  },

  async getTotalByHour({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      eventsSQL.totalByHour,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNHoursData(result.rows, previous);
  },

  async getTotalByDay({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      eventsSQL.totalByDay,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNDaysData(result.rows, previous);
  },

  async getTotalByMonth({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      eventsSQL.totalByMonth,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNMonthsData(result.rows, previous);
  },

  async getAveragePerUserByHour({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      eventsSQL.averagePerUserByHour,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNHoursData(result.rows, previous);
  },

  async getAveragePerUserByDay({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      eventsSQL.averagePerUserByDay,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNDaysData(result.rows, previous);
  },

  async getAveragePerUserByMonth({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      eventsSQL.averagePerUserByMonth,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNMonthsData(result.rows, previous);
  },

  async getMinPerUserByHour({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      eventsSQL.minPerUserByHour,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNHoursData(result.rows, previous);
  },

  async getMinPerUserByDay({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      eventsSQL.minPerUserByDay,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNDaysData(result.rows, previous);
  },

  async getMinPerUserByMonth({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      eventsSQL.minPerUserByMonth,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNMonthsData(result.rows, previous);
  },

  async getMaxPerUserByHour({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      eventsSQL.maxPerUserByHour,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNHoursData(result.rows, previous);
  },

  async getMaxPerUserByDay({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      eventsSQL.maxPerUserByDay,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNDaysData(result.rows, previous);
  },

  async getMaxPerUserByMonth({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      eventsSQL.maxPerUserByMonth,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNMonthsData(result.rows, previous);
  },

  async getMedianPerUserByHour({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      eventsSQL.medianPerUserByHour,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNHoursData(result.rows, previous);
  },

  async getMedianPerUserByDay({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      eventsSQL.medianPerUserByDay,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNDaysData(result.rows, previous);
  },

  async getMedianPerUserByMonth({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      eventsSQL.medianPerUserByMonth,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatEvents.pastNMonthsData(result.rows, previous);
  },
};

const users = {
  async getTotalByHour({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setHours(dateRangeStart.getHours() - previous);
    }

    const result = await dbQuery(
      usersSQL.totalByHour,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatUsers.pastNHoursData(result.rows, previous);
  },

  async getTotalByDay({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setDate(dateRangeStart.getDate() - previous);
    }

    const result = await dbQuery(
      usersSQL.totalByDay,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatUsers.pastNDaysData(result.rows, previous);
  },

  async getTotalByMonth({
    previous,
    event_name,
    filterAttribute,
    filterAttributeValue,
  }) {
    let dateRangeStart;

    if (previous) {
      dateRangeStart = new Date();
      dateRangeStart.setMonth(dateRangeStart.getMonth() - previous);
    }

    const result = await dbQuery(
      usersSQL.totalByMonth,
      dateRangeStart,
      event_name,
      filterAttribute,
      filterAttributeValue,
    );

    return formatUsers.pastNMonthsData(result.rows, previous);
  },
};

module.exports = {
  events,
  users,
};
