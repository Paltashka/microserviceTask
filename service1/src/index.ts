import express from "express";
import swaggerJsDoc from "swagger-jsdoc";
import swaggerUi from "swagger-ui-express";
import { createClient } from "redis";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.json());

const redisClient = createClient({
  url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
});

redisClient.connect().catch(console.error);

const swaggerOptions = {
  swaggerDefinition: {
    info: {
      title: "Microservice API",
      version: "1.0.0",
    },
  },
  apis: ["./src/index.ts"],
};

const swaggerDocs = swaggerJsDoc(swaggerOptions);
app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerDocs));

/**
 * @swagger
 * /operation/sync/{id}:
 *   post:
 *     summary: Sync operation processing
 *     parameters:
 *       - name: id
 *         in: path
 *         required: true
 *         type: string
 *     responses:
 *       200:
 *         description: Operation completed
 *       409:
 *         description: Operation with this ID already exists
 *       429:
 *         description: Too many requests
 *       408:
 *         description: Operation timed out
 */

app.post("/operation/sync/:id", async (req, res) => {
  const { id } = req.params;

  if(!id) return res.status(404).json({ error: "Provide an id in request parameters" });

  const existingOperation = await redisClient.exists(`operation-status:${id}`);
  if (existingOperation) {
    return res.status(409).json({ error: "Operation with this ID already exists" });
  }

  const activeRequests = await redisClient.get("active-requests");
  const maxRequests = parseInt(process.env.MAX_REQUESTS || "5");

  if (parseInt(activeRequests || "0") >= maxRequests) {
    console.log("Too many requests in progress for sync operation.");
    return res.status(429).json({ error: "Too many requests" });
  }

  const operation = req.body;
  operation.id = id;

  console.log("Received sync operation:", operation);

  await redisClient.lPush("sync-queue", JSON.stringify(operation));

  let status = null;
  const start = Date.now();
  const timeout = parseInt(process.env.TIME_BEFORE_TIMEOUT || "12000");

  console.log(`Polling for sync result of operation ${operation.id}`);

  while (Date.now() - start < timeout) {
    status = await redisClient.lPop(`operation-result:${operation.id}`);
    if (status) break;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  if (!status) {
    console.log(`Sync operation ${operation.id} timed out`);
    return res.status(408).json({ error: "Operation timed out" });
  }

  res.status(200).json({ id: operation.id, status });
});

/**
 * @swagger
 * /operation/async/{id}:
 *   post:
 *     summary: Async operation processing
 *     parameters:
 *       - name: id
 *         in: path
 *         required: true
 *         type: string
 *     responses:
 *       200:
 *         description: Operation started
 *       409:
 *         description: Operation with this ID already exists
 *       429:
 *         description: Too many requests
 */

app.post("/operation/async/:id", async (req, res) => {
  const { id } = req.params;

  if(!id) return res.status(404).json({ error: "Provide an id in request parameters" });

  const existingOperation = await redisClient.exists(`operation-status:${id}`);
  if (existingOperation) {
    return res.status(409).json({ error: "Operation with this ID already exists" });
  }

  const activeRequests = await redisClient.get("active-requests");
  const maxRequests = parseInt(process.env.MAX_REQUESTS || "5");

  if (parseInt(activeRequests || "0") >= maxRequests) {
    console.log("Too many requests in progress for async operation.");
    return res.status(429).json({ error: "Too many requests" });
  }

  const data = { id: id, ...req.body };

  await redisClient.rPush("async-queue", JSON.stringify(data));

  res.status(200).json({ message: "Operation started", id });
});

/**
 * @swagger
 * /status/{id}:
 *   get:
 *     summary: Fetch operation status
 *     parameters:
 *       - name: id
 *         in: path
 *         required: true
 *         type: string
 *     responses:
 *       200:
 *         description: Status of the operation
 */

app.get("/status/:id", async (req, res) => {
  const { id } = req.params;

  console.log(`Fetching status for operation ID: ${id}`);

  const status = await redisClient.get(`operation-status:${id}`);

  console.log(`Fetched status for operation ${id}: ${status}`);

  res.status(200).json({ status: status || "Unknown operation" });
});

/**
 * @swagger
 * /clear:
 *   post:
 *     summary: Clear all stored operation statuses and results
 *     responses:
 *       200:
 *         description: All stored statuses and results cleared
 */

app.post("/clear", async (req, res) => {
  try {
    const keys = await redisClient.keys("operation-status:*");

    for (const key of keys) {
      await redisClient.del(key);
    }

    const resultKeys = await redisClient.keys("operation-result:*");
    for (const resultKey of resultKeys) {
      await redisClient.del(resultKey);
    }

    console.log("All stored statuses and results cleared");
    res
      .status(200)
      .json({ message: "All stored statuses and results cleared" });
  } catch (error) {
    console.error("Error clearing stored data:", error);
    res.status(500).json({ error: "Failed to clear stored data" });
  }
});

app.listen(process.env.PORT, () => {
  console.log(`Service 1 running on port ${process.env.PORT}`);
});
