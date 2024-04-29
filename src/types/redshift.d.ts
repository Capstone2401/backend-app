import { TimeUnit } from "./time";

type QueryFunc = (timeUnit: TimeUnit) => string;

interface AggregateEvents {
  total: QueryFunc;
  average: QueryFunc;
  median: QueryFunc;
  minimum: QueryFunc;
  maximum: QueryFunc;
}

interface AggregateUsers extends Pick<AggregateEvents, "total"> {}

type AggregationType = keyof AggregateEvents;

export { AggregateEvents, AggregateUsers, AggregationType };
