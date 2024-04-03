interface TimeUnitValidation {
  hour: true;
  day: true;
  month: true;
}

type ValidTime = "hour" | "day" | "month";
type TimeUnit = keyof TimeUnitValidation;
type DateOffsetMethod = (offset: number, currDate: Date) => Date;

type DateMap<T> = {
  [K in Valid]: T;
};

export { TimeUnitValidation, TimeUnit, DateOffsetMethod, DateMap };
