"use strict";

const express = require("express");
const config = require("./utils/config");
const morgan = require("morgan");
const apiRouter = require("./routes/api");
const { catchAllErrorHandler } = require("./middleware/errors");

const app = express();
app.use(morgan("common"));

const host = config.HOST || "localhost";
const port = config.PORT || 3000;

// routes
app.use("/api", apiRouter);

// Error handler
app.use(catchAllErrorHandler);

// Listener
app.listen(port, host, () => {
  console.log(`App is listening on port ${port} of ${host}.`);
});
