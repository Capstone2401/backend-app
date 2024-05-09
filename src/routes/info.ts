import express from "express";
const router = express.Router();

import handleQueryAttributes from "src/middleware/attributes";
import handleQueryEventNames from "src/middleware/events";

router.get("/eventNames", handleQueryEventNames);
router.get("/attributes", handleQueryAttributes);

export default router;
