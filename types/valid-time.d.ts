interface TimeUnitValidation {
  hour: true;
  day: true;
  month: true;
}

type TimeUnit = keyof TimeUnitValidation;

export { TimeUnitValidation, TimeUnit };
