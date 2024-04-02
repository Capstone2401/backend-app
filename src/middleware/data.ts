import { Request, Response, NextFunction } from "express";
import { QueryModifiers } from "../../types/query-modifiers";
import redshift from "../lib/redshift-pg";

async function handleQueryData(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  let { dateRange, eventName, aggregationType, filters }: QueryModifiers =
    req.body;
  const { timeUnit, previous } = dateRange;
  try {
    const args = [
      timeUnit,
      aggregationType,
      {
        previous,
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
