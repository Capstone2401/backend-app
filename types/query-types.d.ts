import { TimeUnit } from "./time";
import { AggregationType } from "./redshift-types";

interface FilterData {
  [key: string]: string[];
}

interface Filters {
  events: FilterData;
  users: FilterData;
}

interface DateRange {
  timeUnit: TimeUnit;
  previous: number;
}

interface QueryParams {
  dateRange: DateRange;
  eventName?: string;
  aggregationType: AggregationType;
  filters?: Filters;
}

interface QueryArgs {
  timeUnit: TimeUnit;
  aggregationType: AggregationType;
  options?: {
    filters?: Filters;
    previous: number;
    eventName?: string;
  };
}

export { QueryParams, QueryArgs };
