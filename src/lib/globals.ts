import { DateMap } from "src/types/time";

const VALID_TIME_UNIT: DateMap<true> = {
  hour: true,
  day: true,
  month: true,
  week: true,
};

export { VALID_TIME_UNIT };
