import { DateOffsetMethod, DateMap, TimeUnit } from "src/types/time";
import { AggregateEvents } from "src/types/redshift";
import { QueryResultRow } from "pg";

const MS_IN_HOUR = 60 * 60 * 1000; // milliseconds
const MS_IN_DAY = MS_IN_HOUR * 24; // milliseconds
const MS_IN_WEEK = MS_IN_DAY * 7;

type FormatDateMethod = (timestamp: Date) => keyof PastNRecords;
type FormatDate = DateMap<FormatDateMethod>;

interface PastNRecordsValue {
  [key: string]: string | number;
}

type PastNRecords = {
  [key: string]: PastNRecordsValue;
};

type PastNRecordsResult = {
  values: PastNRecordsValue[];
  aggregationType: string;
  timeUnit: TimeUnit;
};

const GENERATE_DATE: DateMap<DateOffsetMethod> = {
  hour: (offset, currDate) =>
    new Date(currDate.getTime() - offset * MS_IN_HOUR),
  day: (offset, currDate) => new Date(currDate.getTime() - offset * MS_IN_DAY),
  month: (offset, currDate) => {
    const monthTimestamp = new Date(currDate);
    monthTimestamp.setMonth(currDate.getMonth() - offset);
    return monthTimestamp;
  },
  week: (offset, currDate) => {
    return new Date(currDate.getTime() - offset * MS_IN_WEEK);
  },
};

const FORMAT_DATE: FormatDate = {
  hour: (timestamp) => timestamp.toISOString().slice(0, 13) + ":00",
  day: (timestamp) => timestamp.toISOString().slice(0, 10),
  month: (timestamp) => timestamp.toISOString().slice(0, 7),
  week: (timestamp) => {
    const today = new Date(timestamp);
    const dayOfWeek = today.getDay(); // Get the current day of the week
    const diff = today.getDate() - dayOfWeek + (dayOfWeek === 0 ? 0 : 1); // Adjust to start of the week
    today.setDate(diff); // Set the date to the start of the week
    return today.toISOString().slice(0, 10); // Return the ISO string representing the start of the week
  },
};

function formatDataBy(
  timeUnit: TimeUnit,
  aggregationType: keyof AggregateEvents,
  records: QueryResultRow[],
  previous: number,
): PastNRecordsResult {
  const currentDate = new Date();
  const pastNRecords: PastNRecords = {};

  for (let idx = 0; idx < previous; idx++) {
    const timestamp = GENERATE_DATE[timeUnit](idx, currentDate);
    const formatted = FORMAT_DATE[timeUnit](timestamp);

    pastNRecords[formatted] = {
      [timeUnit]: formatted,
      [aggregationType]: 0,
    };
  }

  for (const record of records) {
    const recordKey = FORMAT_DATE[timeUnit](record[timeUnit]);
    if (pastNRecords[recordKey]) {
      pastNRecords[recordKey][aggregationType] = Number(
        record.calculated_value,
      );
    }
  }

  return { aggregationType, timeUnit, values: Object.values(pastNRecords) };
}

export default formatDataBy;
