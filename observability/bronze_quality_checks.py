import os
import sys
import json
import logging
from datetime import datetime, timezone

import great_expectations as gx
from great_expectations.dataset import PandasDataset
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── CONFIG ────────────────────────────────────────────────
DELTA_BRONZE_PATH = "/tmp/spark-data/delta/bronze/stock_ticks"
GE_ROOT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "great_expectations"
)
SUITE_NAME = "bronze_stock_ticks_suite"
DATASOURCE_NAME = "bronze_delta_datasource"


# ── DATA QUALITY SUITE ────────────────────────────────────
class BronzeQualityChecker:
    """Runs Great Expectations validations on Bronze Delta table.
    Single responsibility: data quality checks only.

    Production principle:
    Quality checks run BEFORE data moves to Silver.
    Bad data stops here, not downstream.
    """

    def __init__(self):
        logger.info("BronzeQualityChecker initialized.")

    def _setup_datasource(self) -> None:
        """Configure pandas filesystem datasource."""
        try:
            self.context.get_datasource(DATASOURCE_NAME)
            logger.info(f"Datasource '{DATASOURCE_NAME}' already exists.")
        except Exception:
            self.context.sources.add_pandas_filesystem(
                name=DATASOURCE_NAME,
                base_directory=DELTA_BRONZE_PATH
            )
            logger.info(f"Created datasource: {DATASOURCE_NAME}")

    def _build_suite(self) -> None:
        """Build expectation suite programmatically.
        All rules defined as code — versioned in Git.
        """
        try:
            suite = self.context.get_expectation_suite(SUITE_NAME)
            logger.info(f"Suite '{SUITE_NAME}' already exists.")
            self.suite = suite
            return
        except Exception:
            pass

        suite = self.context.add_expectation_suite(
            expectation_suite_name=SUITE_NAME
        )
        self.suite = suite
        logger.info(f"Created suite: {SUITE_NAME}")

    def run_validations(self, df) -> dict:
        """Run all quality checks against a pandas DataFrame."""
        
        # Wrap pandas DataFrame with GE
        ge_df = gx.dataset.PandasDataset(df)

        results = []

        # ── SCHEMA CHECKS ─────────────────────────────────
        logger.info("Running schema checks...")
        required_columns = [
            "symbol", "price", "volume", "event_id",
            "source_timestamp", "ingestion_timestamp",
            "kafka_partition", "kafka_offset"
        ]
        for col in required_columns:
            results.append(
                ge_df.expect_column_to_exist(col)
            )

        # ── NULL CHECKS ───────────────────────────────────
        logger.info("Running null checks...")
        critical_columns = ["symbol", "price", "volume", "event_id"]
        for col in critical_columns:
            results.append(
                ge_df.expect_column_values_to_not_be_null(col)
            )

        # ── TYPE CHECKS ───────────────────────────────────
        logger.info("Running type checks...")
        results.append(
            ge_df.expect_column_values_to_be_of_type("symbol", "str")
        )
        results.append(
            ge_df.expect_column_values_to_be_of_type("price", "float")
        )
        results.append(
            ge_df.expect_column_values_to_be_of_type("volume", "int")
        )

        # ── RANGE CHECKS ──────────────────────────────────
        logger.info("Running range checks...")
        results.append(
            ge_df.expect_column_values_to_be_between(
                "price", min_value=1.0, max_value=10000.0
            )
        )
        results.append(
            ge_df.expect_column_values_to_be_between(
                "volume", min_value=0, max_value=1_000_000
            )
        )

        # ── CATEGORICAL CHECKS ────────────────────────────
        logger.info("Running categorical checks...")
        results.append(
            ge_df.expect_column_values_to_be_in_set(
                "symbol",
                ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
            )
        )

        # ── UNIQUENESS CHECKS ─────────────────────────────
        logger.info("Running uniqueness checks...")
        results.append(
            ge_df.expect_column_values_to_be_unique("event_id")
        )

        # ── VOLUME CHECKS ─────────────────────────────────
        logger.info("Running volume checks...")
        results.append(
            ge_df.expect_table_row_count_to_be_between(
                min_value=1,
                max_value=10_000_000
            )
        )

        # ── SUMMARIZE ─────────────────────────────────────
        passed = sum(1 for r in results if r["success"])
        failed = sum(1 for r in results if not r["success"])
        total = len(results)

        summary = {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "success_rate": round(passed / total * 100, 1),
            "overall_pass": failed == 0,
            "failed_checks": [
                r["expectation_config"]["expectation_type"]
                for r in results if not r["success"]
            ]
        }

        return summary


# ── PIPELINE GATE ─────────────────────────────────────────
class QualityGate:
    """Acts as a gate between Bronze and Silver layers.
    If quality checks fail — pipeline stops here.
    Single responsibility: pass/fail decision only.
    """

    def __init__(self, checker: BronzeQualityChecker):
        self.checker = checker

    def validate_and_gate(self, df) -> bool:
        """Run checks. Return True to proceed, False to stop."""
        logger.info("=" * 50)
        logger.info("BRONZE QUALITY GATE — Starting validation")
        logger.info("=" * 50)

        summary = self.checker.run_validations(df)

        logger.info(f"\nValidation Summary:")
        logger.info(f"  Total checks:   {summary['total_checks']}")
        logger.info(f"  Passed:         {summary['passed']}")
        logger.info(f"  Failed:         {summary['failed']}")
        logger.info(
            f"  Success rate:   {summary['success_rate']}%"
        )

        if summary["overall_pass"]:
            logger.info("\n✅ QUALITY GATE PASSED — Safe to proceed to Silver")
            return True
        else:
            logger.error(
                f"\n❌ QUALITY GATE FAILED — Pipeline stopped"
            )
            logger.error(
                f"Failed checks: {summary['failed_checks']}"
            )
            return False


# ── ENTRY POINT ───────────────────────────────────────────
if __name__ == "__main__":
    import pandas as pd
    from datetime import datetime, timezone, timedelta
    import random

    # Check if Delta table exists
    if not os.path.exists(DELTA_BRONZE_PATH):
        logger.info(
            "Delta table not found. "
            "Creating synthetic test data..."
        )
        # Create synthetic Bronze data matching exact schema
        symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
        now = datetime.now(timezone.utc)
        rows = []
        for i in range(100):
            symbol = random.choice(symbols)
            rows.append({
                "symbol": symbol,
                "price": float(round(random.uniform(100, 500), 2)),
                "volume": random.randint(100, 10000),
                "source_timestamp": (
                    now - timedelta(minutes=i)
                ).isoformat(),
                "event_id": f"{symbol}-{int(now.timestamp() * 1_000_000) + i}",
                "kafka_partition": 0,
                "kafka_offset": i,
                "ingestion_timestamp": now
            })
        df_pandas = pd.DataFrame(rows)
        logger.info(
            f"Created {len(df_pandas)} synthetic records"
        )
    else:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("BronzeQualityCheck") \
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.0.0"
            ) \
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            ) \
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            ) \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Reading Bronze Delta table...")
        df_spark = spark.read.format("delta").load(DELTA_BRONZE_PATH)
        df_pandas = df_spark.toPandas()
        logger.info(
            f"Loaded {len(df_pandas)} records from Bronze layer"
        )
        spark.stop()

    logger.info(f"Running quality checks on {len(df_pandas)} records")

    checker = BronzeQualityChecker()
    gate = QualityGate(checker=checker)
    passed = gate.validate_and_gate(df_pandas)

    if not passed:
        logger.error("Pipeline stopped due to quality failures.")
        sys.exit(1)

    logger.info("Pipeline can proceed to Silver layer.")
    sys.exit(0)

