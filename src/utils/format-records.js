const { VALID_TIME_UNIT } = require("../lib/globals");
const MS_IN_HOUR = 60 * 60 * 1000; // milliseconds
const MS_IN_DAY = 60 * 60 * 24 * 1000; // milliseconds

const GENERATE_DATE = {
  hour: (offset, currDate) =>
    new Date(currDate.getTime() - offset * MS_IN_HOUR),
  day: (offset, currDate) => new Date(currDate.getTime() - offset * MS_IN_DAY),
  month: (offset, currDate) => {
    const monthTimestamp = new Date(currDate);
    monthTimestamp.setMonth(currDate.getMonth() - offset);
    return monthTimestamp;
  },
};

const FORMAT_DATE = {
  hour: (timestamp) => timestamp.toISOString().slice(0, 13) + ":00",
  day: (timestamp) => timestamp.toISOString().slice(0, 10),
  month: (timestamp) => timestamp.toISOString().slice(0, 7),
};

module.exports = function formatDataBy(timeUnit, records, previous) {
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  const currentDate = new Date();
  const pastNRecords = {};

  for (let idx = 0; idx < previous; idx++) {
    const timestamp = GENERATE_DATE[timeUnit](idx, currentDate);
    const formatted = FORMAT_DATE[timeUnit](timestamp);

    pastNRecords[formatted] = {
      [timeUnit]: formatted,
      calculated_value: 0,
    };
  }

  for (const record of records) {
    const recordKey = FORMAT_DATE[timeUnit](record[timeUnit]);
    if (pastNRecords[recordKey]) {
      pastNRecords[recordKey].calculated_value = Number(
        record.calculated_value,
      );
    }
  }

  return Object.values(pastNRecords);
};
