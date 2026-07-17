import { encode } from "@msgpack/msgpack";
import type { ConfirmChannel } from "amqplib";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const content = Buffer.from(JSON.stringify(value));
  return new Promise((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      content,
      {
        contentType: "application/json",
      },
      (err: Error) => {
        if (err !== null) {
          reject(new Error("Message was NACKed by the broker: " + (err as Error).message));
        } else {
          resolve();
        }
      },
    );
  });
}

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  return new Promise((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      Buffer.from(encode(value)),
      { contentType: "application/x-msgpack" },
      (err) => {
        if (err !== null) {
          reject(new Error("Message was NACKed by the broker: " + (err as Error).message));
        } else {
          resolve();
        }
      },
    );
  });
}
