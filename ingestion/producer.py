import json
import time
import random
import sys
import signal
from datetime import datetime, timezone
from confluent_kafka import Producer


# Configuration
TOPIC = "stock.price.ticks"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
BOOTSTRAP_SERVERS = "localhost:9092"


## Tick Generation
class TickGenerator:
    def __init__(self, symbols: list):
        self.symbols = symbols

    def generate(self, symbol: str) -> dict:
        return {
            "symbol": symbol,
            "price": round(random.uniform(100, 500), 2),
            "volume": random.randint(100, 10000),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_id": f"{symbol}-{int(time.time() * 1000000)}",
        }

    def random_symbol(self) -> str:
        return random.choice(self.symbols)


## Kafka Producer


class StockPriceProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = Producer({
        'bootstrap.servers': bootstrap_servers,
        'acks': 'all',
        'retries': 3,
        'enable.idempotence': True,
        'linger.ms': 5,
        'batch.size': 16384,
        'compression.type': 'snappy'
    })

    def publish(self, key: str, value: dict) -> None:
        """Publish a single message to Kafka."""
        self.producer.produce(
            topic=self.topic,
            key=key.encode('utf-8'),
            value=json.dumps(value).encode('utf-8'),
            callback=self._delivery_callback
        )
        self.producer.poll(0)

    def _delivery_callback(self, err, msg):
        """Called when message is delivered or fails."""
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(
                f"Sent: {msg.key().decode()} "
                f"| partition={msg.partition()} "
                f"| offset={msg.offset()}"
            )

    def close(self) -> None:
        """Flush and close the producer cleanly."""
        self.producer.flush()
        print("Producer closed cleanly.")


## Pipeline


class IngestionPipeline:
    def __init__(self, producer: StockPriceProducer, generator: TickGenerator):
        self.producer = producer
        self.generator = generator

    def run(self, num_ticks: int = 10, interval_seconds: int = 1) -> None:
        print(f"Starting Market Pulse ingestion pipeline...")
        try:
            for _ in range(num_ticks):
                symbol = self.generator.random_symbol()
                tick = self.generator.generate(symbol)
                self.producer.publish(key=symbol, value=tick)
                time.sleep(interval_seconds)
        except Exception as e:
            print(f"Error in ingestion pipeline: {e}")
            raise e
        finally:
            self.producer.close()


## Entry Point

if __name__ == "__main__":
    generator = TickGenerator(symbols=SYMBOLS)
    producer = StockPriceProducer(bootstrap_servers=BOOTSTRAP_SERVERS, topic=TOPIC)
    pipeline = IngestionPipeline(producer=producer, generator=generator)
    
    def handle_shutdown(signum, frame):
        print("\nShutdown signal received. Closing producer...")
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Run continuously until Ctrl+C
    pipeline.run(num_ticks=999999, interval_seconds=2)

# Message Schema
