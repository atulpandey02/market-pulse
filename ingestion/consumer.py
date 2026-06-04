import json
from kafka import KafkaConsumer


# Config 
TOPIC = 'stock.price.ticks'
BOOTSTRAP_SERVERS = 'localhost:9092'
GROUP_ID = 'market_pulse_analytics'

# Message Processor
class TickProcessor:
    """ Responsible for processing a single tick message.
    Single responsibility: business logic only.
    """
    def __init__(self):
        self.processed_event_ids = set()

    def is_duplicate(self , event_id:str) -> bool:
        """Check if we have already processed this event."""
        return event_id in self.processed_event_ids

    def process(self , tick:dict) -> None:
        """Process a single tick — idempotent."""
        event_id = tick.get('event_id')

        if self.is_duplicate(event_id):
            print(f'Skipping duplicate event: {event_id}')
            return
        
        # Business Logic here 
        print(f"Processing: {tick['symbol']} @ ${tick['price']}"
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
    def __init__(self , bootstrap_servers: str , topic: str , group_id: str):
        self.topic = topic
        self.running = True
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,

        )
    def close(self) ->None:
        """Close the consumer cleanly."""
        self.running = False
        self.consumer.close()
        print('Consumer closed cleanly')

# CONSUMPTION PIPELINE
class ConsumptionPipeline:
    """Orchestrates message consumption and processing.
    Single responsibility: pipeline coordination only.
    """

    def __init__(self , consumer: StockPriceConsumer , processor: TickProcessor):
        self.consumer = consumer
        self.processor = processor

    def run(self) -> None:
        """Main consumption loop."""
        print(f"Starting consumer | group={GROUP_ID} | topic={TOPIC}")

        try:
            for message in self.consumer.consumer:
                tick = message.value

                # Process FIRST , commit after
                self.processor.process(tick)

                # Manual commit after successful processing
                # at-least-once delivery guarantee
                self.consumer.consumer.commit()

                print(
                    f'Offset commited: partition = {message.partition}'
                    f' | offset={message.offset}'
              )
        except KeyboardInterrupt:
            print('\n Shutdown signal received.')

        except Exception as e:
            print(f"Consumer error : {e}")

        finally :
            self.consumer.close()

# Entry Point
if __name__ == '__main__':
    processor = TickProcessor()
    consumer = StockPriceConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        group_id=GROUP_ID
         )
    pipeline = ConsumptionPipeline(
        consumer=consumer,
        processor=processor
    )
    pipeline.run()