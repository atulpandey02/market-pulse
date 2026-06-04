import json 
import time 
import random
from datetime import datetime, timezone
from kafka import KafkaProducer


# Configuration 
TOPIC = 'stock.price.ticks'
SYMBOLS = ['AAPL' , 'GOOGL' , 'MSFT' , 'AMZN' , 'TSLA']
BOOTSTRAP_SERVERS = 'localhost:9092'

## Tick Generation 
class TickGenerator:
    def __init__(self , symbols: list):
        self.symbols = symbols 

    def generate(self , symbol: str) -> dict:
        return {
        'symbol': symbol,
        'price': round(random.uniform(100 , 500) , 2),
        'volume': random.randint(100 , 10000),
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'event_id': f"{symbol}-{int(time.time() * 1000)}"
    }

    def random_symbol(self) -> str:
        return random.choice(self.symbols)

## Kafka Producer

class StockPriceProducer:
    def __init__(self , bootstrap_servers: str , topic: str):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            acks = 'all' ,
            retries = 3 ,
            enable_idempotence = True)

    def publish(self , key: str , value: dict) -> None:
        future = self.producer.send(
            topic = self.topic,
            key = key,
            value = value
        )
        metadata = future.get(timeout=10)
        print(f"Sent: {value['symbol']} @ ${value['price']}"
            f" | partition={metadata.partition}"
            f" | offset={metadata.offset}")
        
    
    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
        print("Producer closed cleanly.")

## Pipeline 

class IngestionPipeline:
    def __init__(self,producer: StockPriceProducer, generator: TickGenerator):
        self.producer = producer
        self.generator = generator

    def run(self , num_ticks:int = 10 , interval_seconds:int = 1) -> None:
        print(f"Starting Market Pulse ingestion pipeline...")
        try :
            for _ in range(num_ticks):
                symbol = self.generator.random_symbol()
                tick = self.generator.generate(symbol)
                self.producer.publish(key =symbol , value =tick)
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
    pipeline.run(num_ticks=10, interval_seconds=1)
        
        
        


# Message Schema 
