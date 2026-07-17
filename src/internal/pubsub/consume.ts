import amqp, { type Channel, type ChannelModel } from "amqplib";
import { decode } from "@msgpack/msgpack";

export enum SimpleQueueType {
  Durable,
  Transient,
}
export enum ACKType {
  Ack = "acknowledged",
  NackRequeue = "nacked requeued",
  NackDiscard = "nacked discared",
}

export type Handler<T> = (data: T) => ACKType | Promise<ACKType>;
export type Deserializer<T> = (data: Buffer) => T;

export async function declareAndBind(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel();
  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType === SimpleQueueType.Transient,
    exclusive: queueType === SimpleQueueType.Transient,
    arguments: {
      "x-dead-letter-exchange": "peril_dlx",
    },
  });
  ch.bindQueue(queue.queue, exchange, key);
  return [ch, queue];
}

export async function subscribe<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: Handler<T>,
  deserializer: Deserializer<T>,
): Promise<void> {
  const [ch, q] = await declareAndBind(conn, exchange, queueName, key, queueType);

  await ch.prefetch(1);

  const _ = await ch.consume(
    q.queue,
    async (m) => {
      if (!m) return;

      let data: T;
      try {
        data = deserializer(m.content);
      } catch (err) {
        console.error("Could not deserialize message: ", err);
        return;
      }
      try {
        const ack = await handler(data);
        switch (ack) {
          case ACKType.Ack:
            return ch.ack(m);
          case ACKType.NackRequeue:
            return ch.nack(m, false, true);
          case ACKType.NackDiscard:
            return ch.nack(m, false, false);
        }
      } catch (err) {
        console.error("Error handling message: ", err);
        return ch.nack(m, false, false);
      }
    },
    { noAck: false },
  );
}

export async function subscribeJSON<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: Handler<T>,
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => JSON.parse(data.toString()) as T,
  );
}

export async function subscribeMsgPack<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: Handler<T>,
) {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => decode(data) as T,
  );
}
