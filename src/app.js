"use strict";

const express = require("express");
const config = require("./utils/config");
const morgan = require("morgan");
const queryRouter = require("./routes/query");
const infoRouter = require("./routes/info");
const { catchAllErrorHandler } = require("./middleware/errors");

const app = express();
const host = config.HOST || "localhost";
const port = config.PORT || 3000;

app.use(morgan("common"));
app.use(express.json());

// routes
app.use("/api/query", queryRouter);
app.use("/api/info", infoRouter);

// Error handler
app.use(catchAllErrorHandler);

// Listener
app.listen(port, host, () => {
  console.log(`App is listening on port ${port} of ${host}.`);
});
