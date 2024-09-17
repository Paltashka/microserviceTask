import { createClient } from "redis";
import dotenv from "dotenv";

dotenv.config();

const redisClient = createClient({
  url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
});

redisClient.connect().catch(console.error);

const processOperation = async (operation: any, type: string) => {
  console.log(`Processing ${type} operation:`, operation);

  await redisClient.set(`operation-status:${operation.id}`, "Pending");

  await redisClient.incr("active-requests");

  await new Promise((resolve) =>
    setTimeout(resolve, parseInt(process.env.TIMEOUT_TIME || "10000")),
  );

  await redisClient.set(`operation-status:${operation.id}`, "Completed");
  console.log(`Set status for operation ${operation.id}: Completed`);

  if (type === "sync") {
    await redisClient.lPush(`operation-result:${operation.id}`, "Completed");
    console.log(`Pushed sync result for operation ${operation.id} to Redis`);
  }

  await redisClient.decr("active-requests");
};

const processQueues = async () => {
  while (true) {
    const syncOperation = await redisClient.lPop("sync-queue");
    const asyncOperation = await redisClient.lPop("async-queue");

    if (syncOperation || asyncOperation) {

      let operation: any;
      let type: "sync" | "async";

      if (syncOperation) {
        operation = JSON.parse(syncOperation);
        type = "sync";
      } else if (asyncOperation) {
        operation = JSON.parse(asyncOperation);
        type = "async";
      } else {
        console.error("Unexpected: Both operations are null");
        continue;
      }

      processOperation(operation, type).catch(console.error);
    }

    await new Promise((resolve) =>
      setTimeout(
        resolve,
        parseInt(process.env.DELAY_BEFORE_NEXT_CHECK || "100"),
      ),
    );
  }
};

processQueues().catch(console.error);
