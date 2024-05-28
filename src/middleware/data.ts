import { Request, Response, NextFunction } from "express";
import { QueryParams, QueryArgs } from "src/types/query";
import redshift from "src/lib/redshift-pg";

async function handleQueryData(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  let { dateRange, eventName, aggregationType, filters }: QueryParams =
    req.query as unknown as QueryParams;
  const { timeUnit, previous } = dateRange;
  try {
    const args: QueryArgs = {
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
