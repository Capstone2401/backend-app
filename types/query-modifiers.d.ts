interface FilterData {
  [key: string]: string[];
}

interface Filters {
  events: FilterData;
  users: FilterData;
}

interface DateRange {
  timeUnit: "hour" | "day" | "month";
  previous: number;
}

type AggregationType = "total" | "minimum" | "maximum" | "average" | "median";

interface QueryModifiers {
  dateRange: DateRange;
  eventName: string;
  aggregationType: AggregationType;
  filters: Filters;
}

export {
  FilterData,
  Filters,
  QueryModifiers,
  TimeUnitByRange,
  PreviousByRange,
};
