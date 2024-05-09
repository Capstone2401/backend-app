import redshift from "src/lib/redshift-pg";
import { Request, Response, NextFunction } from "express";

async function handleQueryAttributes(
  _req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  try {
    const result = await redshift.getAllAttributes();
    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
}

export default handleQueryAttributes;
