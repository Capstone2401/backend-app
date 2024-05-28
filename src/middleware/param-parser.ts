import qs from "qs";
import { NextFunction, Request, Response } from "express";

export default function paramParser(
  req: Request,
  _: Response,
  next: NextFunction,
): void {
  req.query = qs.parse(req.url.split("?")[1]);
  next();
}
