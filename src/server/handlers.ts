import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";
import { ACKType } from "../internal/pubsub/consume.js";

export async function handlerLog(gl: GameLog): Promise<ACKType> {
  try {
    writeLog(gl);
    return ACKType.Ack;
  } catch (err) {
    console.error("Error writing log: ", err);
    return ACKType.NackDiscard;
  } finally {
    process.stdout.write("> ");
  }
}
