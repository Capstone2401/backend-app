const express = require("express");
const router = express.Router();

const { handleQueryAttributes } = require("../middleware/attributes");
const { handleQueryEventNames } = require("../middleware/events");

router.get("/eventNames", handleQueryEventNames);
router.get("/attributes", handleQueryAttributes);

module.exports = router;
