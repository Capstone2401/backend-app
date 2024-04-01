import { Request, Response, NextFunction } from "express";
import {
  QueryModifiers,
  TimeUnitByRange,
  PreviousByRange,
} from "../../types/query-modifiers";
import redshift from "../lib/redshift-pg";

async function handleQueryData(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  const TIMEUNIT_BY_RANGE: TimeUnitByRange = {
    Today: "hour",
    "7D": "day",
    "30D": "day",
    "3M": "month",
    "6M": "month",
    "12M": "month",
  };

  const PREVIOUS_BY_RANGE: PreviousByRange = {
    Today: 24,
    "7D": 7,
    "30D": 30,
    "3M": 3,
    "6M": 6,
    "12M": 12,
  };

  let { dateRange, eventName, aggregationType, filters }: QueryModifiers =
    req.body;
  try {
    const args = [
      TIMEUNIT_BY_RANGE[dateRange],
      aggregationType,
      {
        previous: PREVIOUS_BY_RANGE[dateRange],
        eventName,
        filters,
      },
    ];

    let result;
    if (req.path === "/users") {
      result = await redshift.getAggregatedUsersBy(args);
    } else {
      result = await redshift.getAggregatedEventsBy(args);
    }

    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
}

export default handleQueryData;
