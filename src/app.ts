import express from "express";
import morgan from "morgan";
import config from "./utils/config";
import queryRouter from "./routes/query";
import infoRouter from "./routes/info";
import { catchAllErrorHandler } from "./middleware/errors";

const app = express();
const host = config.HOST;
const port = config.PORT;

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
