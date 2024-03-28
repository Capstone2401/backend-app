const express = require("express");
const router = express.Router();

const { handleQueryData } = require("../middleware/data");

router.get("/events", handleQueryData);
router.get("/users", handleQueryData);

module.exports = router;
