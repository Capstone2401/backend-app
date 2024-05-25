import express from "express";
const router = express.Router();

import handleQueryData from "src/middleware/data";

router.get("/events", handleQueryData);
router.get("/users", handleQueryData);

export default router;
