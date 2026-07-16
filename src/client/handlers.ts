import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { ACKType } from "../internal/pubsub/consume.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => ACKType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return ACKType.Ack;
  };
}

export function handlerMove(gs: GameState): (am: ArmyMove) => ACKType {
  return (am: ArmyMove) => {
    try {
      const mo = handleMove(gs, am);
      switch (mo) {
        case MoveOutcome.Safe:
          return ACKType.Ack;
        case MoveOutcome.MakeWar:
          return ACKType.Ack;
        default:
          return ACKType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}
