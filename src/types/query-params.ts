import { QueryParams } from "./query";

export default function isQueryParams(params: any): params is QueryParams {
  return (
    params &&
    typeof params === "object" &&
    params.hasOwnProperty("dateRange") &&
    params.dateRange &&
    typeof params.dateRange === "object" &&
    params.dateRange.hasOwnProperty("timeUnit") &&
    params.dateRange.hasOwnProperty("previous") &&
    typeof params.dateRange.timeUnit === "string" &&
    typeof params.dateRange.previous === "number" &&
    params.hasOwnProperty("aggregationType") &&
    params.aggregationType &&
    typeof params.aggregationType === "string" &&
    ["total", "minimum", "median", "maximum", "average"].includes(
      params.aggregationType,
    )
  );
}
