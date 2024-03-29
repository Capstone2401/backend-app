const express = require("express");
const router = express.Router();

const { handleQueryData } = require("../middleware/data");

router.post("/events", handleQueryData);
router.post("/users", handleQueryData);

module.exports = router;
