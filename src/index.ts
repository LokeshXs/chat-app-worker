import { createClient } from "redis";
import dotenv from "dotenv";
import pg from "pg";


dotenv.config();

const redisCli = createClient({
  url:process.env.REDIS_URL
});
const { Client } = pg;
const dbClient = new Client({
  user: process.env.POSTGRES_USERNAME,
  password: process.env.POSTGRES_PASSWORD,
  host: process.env.POSTGRES_HOST,
  port: 5432,
  database: process.env.POSTGRES_DB,
  ssl:true
});

const startWorker = async () => {
  await dbClient.connect();
  console.log("worker is connected to database");
  await redisCli.connect();
  while (1) {
    const message = await redisCli.brPop("messages", 0);

    await new Promise((resolve) => setTimeout(resolve, 1000));

    console.log(message);
    // Send it to Postgres
    if (!message) return;

    try {
      const messageObj = JSON.parse(message.element);
      const dbRes = await dbClient.query(
        `INSERT INTO "Message" (message,"senderId","recepientId","timeDate") VALUES ($1,$2,$3,$4) RETURNING *`,
        [
          messageObj.message,
          messageObj.senderId,
          messageObj.recepientId,
          messageObj.timeDate,
        ]
      );

      // console.log(dbRes);
    } catch (error) {
      console.log(error);
      return;
    }
  }
};

startWorker();
