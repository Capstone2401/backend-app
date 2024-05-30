import { Request, Response, NextFunction } from "express";
import { QueryArgs } from "src/types/query";
import isQueryParams from "src/types/query-params";
import redshift from "src/lib/redshift-pg";
import { ResponseError } from "src/utils/response-error";

async function handleQueryData(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  const queryParams = req.query as unknown;

  if (!isQueryParams(queryParams)) {
    throw new ResponseError({
      message: "Invalid query parameters. Refer to docs for correct format.",
      statusCode: 400,
    });
  }

  const { dateRange, eventName, aggregationType, filters } = queryParams;
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
