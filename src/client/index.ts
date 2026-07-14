import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);

  console.log("Peril game client connected to RabbitMQ");
  ["SIGINT", "SIGTERM"].forEach((signal: string) => {
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed");
      } catch (err) {
        console.error("Error closing RabbitMQ connection: ", err);
      } finally {
        process.exit(0);
      }
    });
  });

  const userName = await clientWelcome();

  // const publishCh = await conn.createConfirmChannel();
  //
  try {
    await declareAndBind(
      conn,
      ExchangePerilDirect,
      `${PauseKey}.${userName}`,
      PauseKey,
      SimpleQueueType.Transient,
    );
  } catch (err) {
    console.log("Error publishing message: ", err);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
