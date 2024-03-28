const express = require("express");
const router = express.Router();

const { handleQueryAttributes } = require("../middleware/attributes");
const {
  handleQueryEventNames,
  handleQueryEvents,
} = require("../middleware/events");

router.get("/allEventNames", handleQueryEventNames);
router.get("/events", handleQueryEvents);
router.get("/attributes", handleQueryAttributes);

module.exports = router;
