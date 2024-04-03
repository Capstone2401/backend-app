import { Request, Response, NextFunction } from "express";
import { QueryParameters, QueryArguments } from "../../types/query-types";
import redshift from "../lib/redshift-pg";

async function handleQueryData(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  let { dateRange, eventName, aggregationType, filters }: QueryParameters =
    req.body;
  const { timeUnit, previous } = dateRange;
  try {
    const args: QueryArguments = {
      timeUnit,
      aggregationType,
      options: {
        previous,
        eventName,
        filters,
      },
    };

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
