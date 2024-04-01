import { Request, Response, NextFunction } from "express";

function catchAllErrorHandler(
  err: Error,
  _req: Request,
  res: Response,
  _next: NextFunction,
): void {
  console.error(err); // Writes more extensive information to the console log
  res.status(404).send(err.message); // TODO; Tranform to more granular error messages for 4XX and 5XX
}

export { catchAllErrorHandler };
