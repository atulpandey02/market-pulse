import json
from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Consumer, TopicPartition


# Config
TOPIC = "stock.price.ticks"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "market_pulse_analytics"


# Message Processor
class TickProcessor:
    """Responsible for processing a single tick message.
    Single responsibility: business logic only.
    """

    def __init__(self):
        self.processed_event_ids = set()

    def is_duplicate(self, event_id: str) -> bool:
        """Check if we have already processed this event."""
        return event_id in self.processed_event_ids

    def process(self, tick: dict) -> None:
        """Process a single tick — idempotent."""
        event_id = tick.get("event_id")

        if self.is_duplicate(event_id):
            print(f"Skipping duplicate event: {event_id}")
            return

        # Business Logic here
        print(
            f"Processing: {tick['symbol']} @ ${tick['price']}"
            f" | volume = {tick['volume']}"
            f" | event_id = {event_id}"
        )

        # Mark as processed AFTER successful processing
        self.processed_event_ids.add(event_id)


# Kafka Consumer
class StockPriceConsumer:
    """Responsible for connecting to Kafka and consuming ticks.
    Single responsibility: message consumption only.
    """

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.topic = topic
        self.kafka_consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        self.kafka_consumer.subscribe([topic])

    def close(self) -> None:
        """Close the consumer cleanly."""
        self.running = False
        self.consumer.close()
        print("Consumer closed cleanly")


# CONSUMPTION PIPELINE
class ConsumptionPipeline:
    """Orchestrates message consumption and processing.
    Single responsibility: pipeline coordination only.
    """

    def __init__(self, consumer: StockPriceConsumer, processor: TickProcessor):
        self.consumer = consumer
        self.processor = processor

    def run(self) -> None:
        print(f"Starting consumer | group={GROUP_ID} | topic={TOPIC}")
        print("Waiting for messages...\n")

        try:
            while True:
                msg = self.consumer.kafka_consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                tick = json.loads(msg.value().decode('utf-8'))

                processed = self.processor.process(tick)

                if processed:
                    self.consumer.kafka_consumer.commit(
                        offsets=[
                            TopicPartition(
                                msg.topic(),
                                msg.partition(),
                                msg.offset() + 1
                            )
                        ]
                    )
                    print(
                        f"Offset committed:"
                        f" partition={msg.partition()}"
                        f" | offset={msg.offset()}\n"
                    )

        except KeyboardInterrupt:
            print("\nShutdown signal received.")

        finally:
            self.consumer.close()


# Entry Point
if __name__ == "__main__":
    processor = TickProcessor()
    consumer = StockPriceConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS, topic=TOPIC, group_id=GROUP_ID
    )
    pipeline = ConsumptionPipeline(consumer=consumer, processor=processor)
    pipeline.run()
