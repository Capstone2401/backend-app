const redshift = require("../lib/redshift-pg");

async function handleQueryEventNames(_req, res, next) {
  try {
    let result = await redshift.getAllEventNames();
    result = result.map((entry) => entry.event_name);
    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
}

module.exports = {
  handleQueryEventNames,
};
