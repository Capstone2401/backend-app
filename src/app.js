const express = require("express");
const config = require("./utils/config");

const app = express();
const host = config.HOST || "localhost";
const port = config.PORT || 3000;

// Error handler
app.use((err, _req, res, _next) => {
  console.log(err); // Writes more extensive information to the console log
  res.status(404).send(err.message);
});

// Listener
app.listen(port, host, () => {
  console.log(`App is listening on port ${port} of ${host}.`);
});
