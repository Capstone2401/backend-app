interface TimeUnitValidation {
  hour: true;
  day: true;
  month: true;
}

type TimeUnit = "hour" | "day" | "month";
type DateOffsetMethod = (offset: number, currDate: Date) => Date;

type DateMap<T> = {
  [K in TimeUnit]: T;
};

export { TimeUnitValidation, TimeUnit, DateOffsetMethod, DateMap };
