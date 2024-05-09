import log from "src/utils/log";
import { Request, Response, NextFunction } from "express";
import { ResponseError } from "src/utils/response-error";

function catchAllErrorHandler(
  error: Error | ResponseError,
  _req: Request,
  res: Response,
  _next: NextFunction,
): void {
  log.error(error);

  if (error instanceof ResponseError) {
    res.status(error.statusCode).send({
      error: error.message,
    });
  } else {
    res.status(500).send({ error: error.message });
  }
}

export { catchAllErrorHandler };
