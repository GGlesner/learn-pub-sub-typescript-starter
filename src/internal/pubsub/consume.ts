import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}
export enum ACKType {
  Ack = "acknowledged",
  NackRequeue = "nacked requeued",
  NackDiscard = "nacked discared",
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
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

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => ACKType,
): Promise<void> {
  const [ch, q] = await declareAndBind(conn, exchange, queueName, key, queueType);
  const _ = await ch.consume(q.queue, (m) => {
    if (!m) return;

    let data: T;
    try {
      data = JSON.parse(m.content.toString());
    } catch (err) {
      console.error("Could not unmarshal message: ", err);
      return;
    }
    try {
      const ack = handler(data);
      console.log(ack);
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
      ch.nack(m, false, false);
      return;
    }
  });
}
