interface FilterData {
  [key: string]: string[];
}

interface Filters {
  events: FilterData;
  users: FilterData;
}

interface QueryModifiers {
  dateRange: keyof TimeUnitByRange;
  eventName: string;
  aggregationType: string;
  filters: Filters;
}

interface TimeUnitByRange {
  Today: "hour";
  "7D": "day";
  "30D": "day";
  "3M": "month";
  "6M": "month";
  "12M": "month";
}

type PreviousByRange = {
  [Key in keyof TimeUnitByRange]: number;
};

export {
  FilterData,
  Filters,
  QueryModifiers,
  TimeUnitByRange,
  PreviousByRange,
};
