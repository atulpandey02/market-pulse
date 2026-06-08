import json
import logging
from datetime import datetime
from pathlib import Path
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer
)
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── CONFIG ────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "stock.price.ticks"
SPIKE_THRESHOLD_PCT = 3.0  # Alert if price > 3% above window avg


# ── TIMESTAMP ASSIGNER ────────────────────────────────────
class TickTimestampAssigner(TimestampAssigner):
    """Extract event time from tick's source_timestamp.
    This enables event-time windowing — correct for financial data.
    """

    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        """Return event time in milliseconds."""
        try:
            dt = datetime.fromisoformat(
                value["timestamp"].replace("Z", "+00:00")
            )
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp


# ── OHLCV AGGREGATION ─────────────────────────────────────
class OHLCVAggregate(AggregateFunction):
    """Aggregates ticks into OHLCV candle within a window.
    Single responsibility: accumulate price/volume data.

    OHLCV = Open, High, Low, Close, Volume
    """

    def create_accumulator(self):
        """Initial empty accumulator."""
        return {
            "open": None,
            "high": float("-inf"),
            "low": float("inf"),
            "close": None,
            "volume": 0,
            "count": 0,
            "sum_price": 0.0
        }

    def add(self, value: dict, accumulator: dict) -> dict:
        """Add one tick to the accumulator."""
        price = value["price"]
        volume = value["volume"]

        if accumulator["open"] is None:
            accumulator["open"] = price

        accumulator["high"] = max(accumulator["high"], price)
        accumulator["low"] = min(accumulator["low"], price)
        accumulator["close"] = price
        accumulator["volume"] += volume
        accumulator["count"] += 1
        accumulator["sum_price"] += price

        return accumulator

    def get_result(self, accumulator: dict) -> dict:
        """Return final OHLCV result."""
        if accumulator["count"] == 0:
            return accumulator
        accumulator["avg_price"] = round(
            accumulator["sum_price"] / accumulator["count"], 2
        )
        return accumulator

    def merge(self, acc1: dict, acc2: dict) -> dict:
        """Merge two accumulators (needed for session windows)."""
        return {
            "open": acc1["open"] or acc2["open"],
            "high": max(acc1["high"], acc2["high"]),
            "low": min(acc1["low"], acc2["low"]),
            "close": acc2["close"],
            "volume": acc1["volume"] + acc2["volume"],
            "count": acc1["count"] + acc2["count"],
            "sum_price": acc1["sum_price"] + acc2["sum_price"]
        }


# ── ALERT PROCESSOR ───────────────────────────────────────
class PriceAlertProcessor(ProcessWindowFunction):
    """Processes completed window results and fires alerts.
    Single responsibility: alert detection logic only.
    """

    def process(self, key, context, elements):
        """Called once per window per key when window closes."""
        for ohlcv in elements:
            window = context.window()
            window_start = datetime.fromtimestamp(
                window.start / 1000
            ).strftime("%H:%M:%S")
            window_end = datetime.fromtimestamp(
                window.end / 1000
            ).strftime("%H:%M:%S")

            spread_pct = 0.0
            if ohlcv["low"] > 0:
                spread_pct = round(
                    (ohlcv["high"] - ohlcv["low"])
                    / ohlcv["low"] * 100, 2
                )

            result = {
                "symbol": key,
                "window": f"{window_start}-{window_end}",
                "open": ohlcv["open"],
                "high": ohlcv["high"],
                "low": ohlcv["low"],
                "close": ohlcv["close"],
                "avg_price": ohlcv.get("avg_price", 0),
                "volume": ohlcv["volume"],
                "tick_count": ohlcv["count"],
                "spread_pct": spread_pct,
                "alert": spread_pct > SPIKE_THRESHOLD_PCT
            }

            alert_prefix = "🚨 ALERT" if result["alert"] else "📊 CANDLE"
            logger.info(
                f"{alert_prefix} | {result['symbol']} | "
                f"Window: {result['window']} | "
                f"O:{result['open']} H:{result['high']} "
                f"L:{result['low']} C:{result['close']} | "
                f"Avg:{result['avg_price']} | "
                f"Vol:{result['volume']} | "
                f"Spread:{result['spread_pct']}%"
            )

            yield json.dumps(result)


# ── FLINK PIPELINE ────────────────────────────────────────
class FlinkPriceAlertPipeline:
    """Orchestrates the Flink streaming pipeline.
    Single responsibility: pipeline coordination only.
    """

    def __init__(self):
        self.env = StreamExecutionEnvironment\
            .get_execution_environment()
        self.env.set_parallelism(1)
        self.env.enable_checkpointing(30000)  # checkpoint every 30s

        # Add Kafka connector JAR
        import os
        jar_dir = "/tmp/flink-jars"
        jars = [
            "flink-kafka.jar",
            "kafka-clients.jar"
        ]

        jar_urls = []
        for jar in jars:
            jar_path = os.path.join(jar_dir, jar)
            if not os.path.exists(jar_path):
                raise FileNotFoundError(f"JAR not found: {jar_path}")
            jar_urls.append(f"file://{jar_path}")
            print(f"Loaded JAR: {jar}")

            
            self.env.add_jars(*jar_urls)

    def build_kafka_source(self) -> KafkaSource:
        """Build Kafka source with event time semantics."""
        return (
            KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
            .set_topics(KAFKA_TOPIC)
            .set_group_id("flink-price-alerts")
            .set_starting_offsets(
                KafkaOffsetsInitializer.earliest()
            )
            .set_value_only_deserializer(
                SimpleStringSchema()
            )
            .build()
        )

    def run(self) -> None:
        """Build and execute the Flink pipeline."""

        kafka_source = (
            KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
            .set_topics(KAFKA_TOPIC)
            .set_group_id("flink-price-alerts")
            .set_starting_offsets(
                KafkaOffsetsInitializer.earliest()
            )
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )

        watermark_strategy = (
            WatermarkStrategy
            .for_bounded_out_of_orderness(
                Duration.of_seconds(10)
            )
            .with_timestamp_assigner(TickTimestampAssigner())
        )

        stream = (
            self.env
            .from_source(
                kafka_source,
                watermark_strategy,
                "Kafka Stock Ticks"
            )
            .map(lambda x: json.loads(x))
            .key_by(lambda tick: tick["symbol"])
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(
                OHLCVAggregate(),
                PriceAlertProcessor()
            )
        )

        stream.print()

        logger.info("Starting Flink Price Alert Pipeline...")
        logger.info(f"Topic: {KAFKA_TOPIC}")
        logger.info("Windowing: 1-minute tumbling event-time windows")
        logger.info(
            f"Alert threshold: {SPIKE_THRESHOLD_PCT}% spread"
        )

        self.env.execute("MarketPulse-Price-Alerts")


# ── ENTRY POINT ───────────────────────────────────────────
if __name__ == "__main__":
    pipeline = FlinkPriceAlertPipeline()
    pipeline.run()