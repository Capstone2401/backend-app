function pastNHoursData(records, previous) {
  const currentDate = new Date();

  const pastNHours = {};

  for (let idx = 0; idx < previous; idx++) {
    const hour = 1000 * 60 * 60;
    const hourTimestamp = new Date(currentDate.getTime() - idx * hour);

    const formattedTimestamp =
      hourTimestamp.toISOString().replace("T", " ").slice(0, 13) + ":00";

    pastNHours[formattedTimestamp] = {
      event_hour: formattedTimestamp,
      calculated_value: 0,
    };
  }

  for (const record of records) {
    const hourKey = record.event_hour.slice(0, 13) + ":00";
    if (pastNHours[hourKey]) {
      pastNHours[hourKey].calculated_value = Number(record.calculated_value);
    }
  }

  return Object.values(pastNHours);
}

function pastNDaysData(records, previous) {
  const currentDate = new Date();

  const pastNDays = {};

  for (let idx = 0; idx < previous; idx++) {
    const day = 1000 * 60 * 60 * 24;
    const dayTimestamp = new Date(currentDate.getTime() - idx * day);

    const formattedTimestamp = dayTimestamp.toISOString().slice(0, 10); // Formatting to 'YYYY-MM-DD'

    pastNDays[formattedTimestamp] = {
      event_day: formattedTimestamp,
      calculated_value: 0,
    };
  }

  // Loop through the records and insert them into the right day spots
  for (const record of records) {
    const dayKey = record.event_day.slice(0, 10); // Extracting only date part
    if (pastNDays[dayKey]) {
      pastNDays[dayKey].calculated_value = Number(record.calculated_value);
    }
  }

  return Object.values(pastNDays);
}

function pastNMonthsData(records, previous) {
  const currentDate = new Date();

  const pastNMonths = {};

  for (let idx = 0; idx < previous; idx++) {
    const monthTimestamp = new Date(currentDate);
    monthTimestamp.setMonth(currentDate.getMonth() - idx);

    const year = monthTimestamp.getFullYear();
    const month = (monthTimestamp.getMonth() + 1).toString().padStart(2, 0);
    const formattedTimestamp = `${year}-${month}`;

    pastNMonths[formattedTimestamp] = {
      event_month: formattedTimestamp,
      calculated_value: 0,
    };
  }

  for (const record of records) {
    const monthKey = record.event_month.slice(0, 7); // Extracting only year and month part
    if (pastNMonths[monthKey]) {
      pastNMonths[monthKey].calculated_value = Number(record.calculated_value);
    }
  }

  return Object.values(pastNMonths);
}

module.exports = {
  pastNHoursData,
  pastNDaysData,
  pastNMonthsData,
};
