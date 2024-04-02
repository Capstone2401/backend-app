const redshift = require("../lib/redshift-pg");
async function handleQueryData(req, res, next) {
  const { dateRange, eventName, aggregationType, filters } = req.body;
  const { timeUnit, previous } = dateRange;

  try {
    const args = [
      timeUnit,
      aggregationType,
      {
        previous,
        eventName,
        filters,
      },
    ];

    let result;
    if (req.path === "/users") {
      result = await redshift.getAggregatedUsersBy(...args);
    } else {
      result = await redshift.getAggregatedEventsBy(...args);
    }

    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
}

module.exports = { handleQueryData };
