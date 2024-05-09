import express from "express";
const router = express.Router();

import handleQueryData from "src/middleware/data";

router.post("/events", handleQueryData);
router.post("/users", handleQueryData);

export default router;
