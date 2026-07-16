import amqp from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { ACKType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => ACKType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return ACKType.Ack;
  };
}

export function handlerMove(
  gs: GameState,
  ch: amqp.ConfirmChannel,
): (am: ArmyMove) => Promise<ACKType> {
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
          } catch (err) {
            console.error("Error publishing war recognition: ", err);
          } finally {
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

export function handlerWar(gs: GameState): (rw: RecognitionOfWar) => Promise<ACKType> {
  return async (rw: RecognitionOfWar) => {
    try {
      const wr = handleWar(gs, rw);
      switch (wr.result) {
        case WarOutcome.NotInvolved:
          return ACKType.NackRequeue;
        case WarOutcome.OpponentWon:
          return ACKType.Ack;
        case WarOutcome.YouWon:
          return ACKType.Ack;
        case WarOutcome.Draw:
          return ACKType.Ack;
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
