import redshift from "src/lib/redshift-pg";
import { Request, Response, NextFunction } from "express";

async function handleQueryEventNames(
  _req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  try {
    let result = await redshift.getAllEventNames();
    result = result.map((entry) => entry.event_name);
    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
}

export default handleQueryEventNames;
