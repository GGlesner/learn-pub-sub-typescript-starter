import { type ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { ACKType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { publishGameLog } from "./index.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => ACKType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return ACKType.Ack;
  };
}

export function handlerMove(gs: GameState, ch: ConfirmChannel): (am: ArmyMove) => Promise<ACKType> {
  return async (am: ArmyMove) => {
    try {
      const mo = handleMove(gs, am);
      switch (mo) {
        case MoveOutcome.Safe:
          return ACKType.Ack;
        case MoveOutcome.MakeWar:
          const rw: RecognitionOfWar = {
            attacker: am.player,
            defender: gs.getPlayerSnap(),
          };
          try {
            await publishJSON(ch, ExchangePerilTopic, `${WarRecognitionsPrefix}.${am.player}`, rw);
            return ACKType.Ack;
          } catch (err) {
            console.error("Error publishing war recognition: ", err);
            return ACKType.NackRequeue;
          }
        default:
          return ACKType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}

export function handlerWar(
  gs: GameState,
  ch: ConfirmChannel,
): (rw: RecognitionOfWar) => Promise<ACKType> {
  return async (rw: RecognitionOfWar) => {
    try {
      const wr = handleWar(gs, rw);
      switch (wr.result) {
        case WarOutcome.NotInvolved:
          return ACKType.NackRequeue;
        case WarOutcome.OpponentWon:
          try {
            await publishGameLog(
              ch,
              gs.getUsername(),
              `${wr.winner} won a war against ${wr.loser}`,
            );
            return ACKType.Ack;
          } catch (err) {
            console.error("Error publishing war game log: ", err);
            return ACKType.NackRequeue;
          }
        case WarOutcome.YouWon:
          try {
            await publishGameLog(
              ch,
              gs.getUsername(),
              `${wr.winner} won a war against ${wr.loser}`,
            );
            return ACKType.Ack;
          } catch (err) {
            console.error("Error publishing war game log: ", err);
            return ACKType.NackRequeue;
          }
        case WarOutcome.Draw:
          try {
            publishGameLog(
              ch,
              gs.getUsername(),
              `A war between ${wr.attacker} and ${wr.defender} resulted in a draw`,
            );
            return ACKType.Ack;
          } catch (err) {
            console.error("Error publishing war game log: ", err);
            return ACKType.NackRequeue;
          }
        case WarOutcome.NoUnits:
          return ACKType.NackDiscard;
        default:
          console.error("Unknown war result");
          return ACKType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}
