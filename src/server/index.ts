import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
} from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType, subscribeMsgPack } from "../internal/pubsub/consume.js";
import { handlerLog } from "./handlers.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);

  console.log("Peril game server connected to RabbitMQ");
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

  await subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    GameLogSlug + ".*",
    SimpleQueueType.Durable,
    handlerLog,
  );

  printServerHelp();

  const publishCh = await conn.createConfirmChannel();

  while (true) {
    const inputs = await getInput();
    if (inputs.length === 0) {
      continue;
    }
    const cmd = inputs[0];
    if (cmd === "pause") {
      console.log("sending a pause message");
      try {
        await publishJSON(publishCh, ExchangePerilDirect, PauseKey, {
          isPaused: true,
        });
      } catch (err) {
        console.log("Error publishing message: ", err);
      }
    } else if (cmd === "resume") {
      console.log("sending a resume message");
      try {
        await publishJSON(publishCh, ExchangePerilDirect, PauseKey, {
          isPaused: false,
        });
      } catch (err) {
        console.log("Error publishing message: ", err);
      }
    } else if (cmd === "quit") {
      console.log("exiting");
      process.exit(0);
    } else {
      console.log("did not understand command, try again");
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
