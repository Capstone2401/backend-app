const redshift = require("../lib/redshift-pg");

async function handleQueryAttributes(_req, res, next) {
  try {
    const result = await redshift.getAllAttributes();
    res.status(200).send(JSON.stringify(result));
  } catch (error) {
    next(error);
  }
}

module.exports = { handleQueryAttributes };
