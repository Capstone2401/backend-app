import express from "express";
const router = express.Router();

import handleQueryAttributes from "../middleware/attributes";
import handleQueryEventNames from "../middleware/events";

router.get("/eventNames", handleQueryEventNames);
router.get("/attributes", handleQueryAttributes);

export default router;
