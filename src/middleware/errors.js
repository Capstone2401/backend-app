function catchAllErrorHandler(err, _req, res, _next) {
  console.error(err); // Writes more extensive information to the console log
  res.status(404).send(err.message); // TODO; Tranform to more granular error messages for 4XX and 5XX
}

module.exports = { catchAllErrorHandler };
