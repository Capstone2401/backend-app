interface TimeUnitValidation {
  hour: true;
  day: true;
  week: true;
  month: true;
}

type TimeUnit = "hour" | "day" | "week" | "month";
type DateOffsetMethod = (offset: number, currDate: Date) => Date;

type DateMap<T> = {
  [K in TimeUnit]: T;
};

export { TimeUnitValidation, TimeUnit, DateOffsetMethod, DateMap };
