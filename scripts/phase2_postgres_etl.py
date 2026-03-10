"""
phase2_postgres_etl.py

Why this file exists:
Phase 2 converts the cleaned CSV artifact produced in Phase 1 into a PostgreSQL
analytical table suitable for indexing, benchmarking, and SQL-based analytics.

Why this architecture is used:
A staging-to-final ETL pattern is more robust than direct row-wise insertion.
It preserves bulk-load performance via COPY while ensuring the final benchmark
table has explicit PostgreSQL types, especially TIMESTAMP for datetime fields.

Project scope fit:
This script implements only Phase 2:
1. schema creation
2. raw CSV loading
3. explicit type casting
4. row-count verification
It does not yet create indexes or benchmark queries.
"""

from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Any

import psycopg2


# -----------------------------------------------------------------------------
# Configuration
# Why:
# These values isolate environment-specific settings from the ETL logic so the
# script remains portable and easier to audit.
# -----------------------------------------------------------------------------
CSV_FILE = Path(
    r"C:\Users\Asus\OneDrive\Desktop\taxi_data\taxi_project_output\yellow_taxi_2024_cleaned.csv"
)

LOG_FILE = Path(
    r"C:\Users\Asus\OneDrive\Desktop\taxi_data\taxi_project_output\phase2_postgres_etl.log"
)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "taxi_project",
    "user": "postgres",
    "password": "1234"
}


# -----------------------------------------------------------------------------
# Schema DDL
# Why:
# Staging and analytics are separated to preserve fast raw ingestion while still
# enforcing the typed schema needed for valid SQL benchmarking later.
# -----------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS staging.yellow_taxi_trips_2024_raw;

CREATE TABLE staging.yellow_taxi_trips_2024_raw (
    vendorid TEXT,
    tpep_pickup_datetime TEXT,
    tpep_dropoff_datetime TEXT,
    passenger_count TEXT,
    trip_distance TEXT,
    ratecodeid TEXT,
    store_and_fwd_flag TEXT,
    pulocationid TEXT,
    dolocationid TEXT,
    payment_type TEXT,
    fare_amount TEXT,
    extra TEXT,
    mta_tax TEXT,
    tip_amount TEXT,
    tolls_amount TEXT,
    improvement_surcharge TEXT,
    total_amount TEXT,
    congestion_surcharge TEXT,
    airport_fee TEXT
);

DROP TABLE IF EXISTS analytics.yellow_taxi_trips_2024;

CREATE TABLE analytics.yellow_taxi_trips_2024 (
    trip_id BIGSERIAL PRIMARY KEY,
    vendorid SMALLINT,
    tpep_pickup_datetime TIMESTAMP NOT NULL,
    tpep_dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION NOT NULL,
    ratecodeid SMALLINT,
    store_and_fwd_flag VARCHAR(1),
    pulocationid INTEGER,
    dolocationid INTEGER,
    payment_type SMALLINT,
    fare_amount NUMERIC(10, 2) NOT NULL,
    extra NUMERIC(10, 2),
    mta_tax NUMERIC(10, 2),
    tip_amount NUMERIC(10, 2),
    tolls_amount NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    congestion_surcharge NUMERIC(10, 2),
    airport_fee NUMERIC(10, 2)
);
"""


# -----------------------------------------------------------------------------
# Typed transform SQL
# Why:
# Explicit casting ensures the final table has a deterministic schema. This is
# especially critical for datetime fields, which must be TIMESTAMP for later
# index-based time filtering benchmarks.
# -----------------------------------------------------------------------------
TRANSFORM_SQL = """
INSERT INTO analytics.yellow_taxi_trips_2024 (
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
)
SELECT
    NULLIF(vendorid,'')::NUMERIC::SMALLINT,
    NULLIF(tpep_pickup_datetime,'')::TIMESTAMP,
    NULLIF(tpep_dropoff_datetime,'')::TIMESTAMP,
    NULLIF(passenger_count,'')::NUMERIC::INTEGER,
    NULLIF(trip_distance,'')::DOUBLE PRECISION,
    NULLIF(ratecodeid,'')::NUMERIC::SMALLINT,
    NULLIF(store_and_fwd_flag,'')::VARCHAR(1),
    NULLIF(pulocationid,'')::NUMERIC::INTEGER,
    NULLIF(dolocationid,'')::NUMERIC::INTEGER,
    NULLIF(payment_type,'')::NUMERIC::SMALLINT,
    NULLIF(fare_amount,'')::NUMERIC(10,2),
    NULLIF(extra,'')::NUMERIC(10,2),
    NULLIF(mta_tax,'')::NUMERIC(10,2),
    NULLIF(tip_amount,'')::NUMERIC(10,2),
    NULLIF(tolls_amount,'')::NUMERIC(10,2),
    NULLIF(improvement_surcharge,'')::NUMERIC(10,2),
    NULLIF(total_amount,'')::NUMERIC(10,2),
    NULLIF(congestion_surcharge,'')::NUMERIC(10,2),
    NULLIF(airport_fee,'')::NUMERIC(10,2)
FROM staging.yellow_taxi_trips_2024_raw
WHERE NULLIF(tpep_pickup_datetime,'') IS NOT NULL
  AND NULLIF(tpep_dropoff_datetime,'') IS NOT NULL;
"""

RAW_COUNT_SQL = "SELECT COUNT(*) FROM staging.yellow_taxi_trips_2024_raw;"
FINAL_COUNT_SQL = "SELECT COUNT(*) FROM analytics.yellow_taxi_trips_2024;"

DATATYPE_CHECK_SQL = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'yellow_taxi_trips_2024'
  AND column_name IN ('tpep_pickup_datetime', 'tpep_dropoff_datetime')
ORDER BY column_name;
"""


def configure_logging() -> logging.Logger:
    """
    Why logging is configured centrally:
    ETL phases need durable diagnostics for connectivity, schema creation, bulk
    loading, transformation timing, and row reconciliation.
    """
    logger = logging.getLogger("phase2")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


@dataclass
class ETLStats:
    """
    Why stats are captured:
    Phase 2 should generate measurable evidence showing ETL throughput and row
    reconciliation, which directly supports the project's optimization narrative.
    """
    csv_exists: bool = False
    csv_size_mb: float = 0.0
    schema_seconds: float = 0.0
    copy_seconds: float = 0.0
    transform_seconds: float = 0.0
    raw_row_count: int = 0
    final_row_count: int = 0
    started_at_epoch: float = 0.0
    ended_at_epoch: float = 0.0

    @property
    def duration_seconds(self) -> float:
        """
        Why runtime is derived:
        Using start/end timestamps prevents manual timing drift and keeps the
        reported ETL duration aligned with actual execution.
        """
        return round(self.ended_at_epoch - self.started_at_epoch, 3)


class PostgresTaxiETL:
    """
    Why this class exists:
    The ETL lifecycle is cohesive: validate artifact, connect, create schema,
    bulk load, transform to typed table, verify outcome. One class keeps that
    flow maintainable without unnecessary abstraction.
    """

    def __init__(self, db_config: Dict[str, Any], csv_file: Path, logger: logging.Logger) -> None:
        """
        Why dependencies are injected:
        This keeps operational configuration separate from ETL behavior and makes
        the script easier to adapt across environments.
        """
        self.db_config = db_config
        self.csv_file = csv_file
        self.logger = logger
        self.stats = ETLStats()

    def validate_csv(self) -> None:
        """
        Why this check happens first:
        Phase 2 depends entirely on the Phase 1 output artifact, so the ETL must
        fail immediately if that artifact is missing or empty.
        """
        if not self.csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_file}")

        if self.csv_file.stat().st_size == 0:
            raise ValueError(f"CSV file is empty: {self.csv_file}")

        self.stats.csv_exists = True
        self.stats.csv_size_mb = round(self.csv_file.stat().st_size / (1024 * 1024), 3)

        self.logger.info("Validated CSV file: %s", self.csv_file)
        self.logger.info("CSV size: %.3f MB", self.stats.csv_size_mb)

    def connect(self):
        """
        Why connection setup is isolated:
        Connectivity issues are operational failures and should be diagnosable
        independently from the ETL transformation logic.
        """
        return psycopg2.connect(**self.db_config)

    def create_schema(self, conn) -> None:
        """
        Why DDL is executed up front:
        Bulk ingestion should only begin once the landing and target table
        structures are known to be correct.
        """
        self.logger.info("Creating schemas and tables.")
        start = time.time()

        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)

        conn.commit()
        self.stats.schema_seconds = round(time.time() - start, 3)
        self.logger.info("Schema creation completed in %.3f seconds.", self.stats.schema_seconds)

    def copy_to_staging(self, conn) -> None:
        """
        Why COPY is used:
        COPY is the correct PostgreSQL primitive for loading very large CSV files.
        It materially outperforms Python-driven row inserts and reduces client-side
        overhead.
        """
        self.logger.info("Starting COPY into staging table.")
        start = time.time()

        copy_sql = """
        COPY staging.yellow_taxi_trips_2024_raw (
            vendorid,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            ratecodeid,
            store_and_fwd_flag,
            pulocationid,
            dolocationid,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee
        )
        FROM STDIN WITH (
            FORMAT CSV,
            HEADER TRUE
        );
        """

        with conn.cursor() as cur:
            with self.csv_file.open("r", encoding="utf-8", newline="") as f:
                cur.copy_expert(copy_sql, f)

        conn.commit()
        self.stats.copy_seconds = round(time.time() - start, 3)
        self.logger.info("COPY completed in %.3f seconds.", self.stats.copy_seconds)

    def transform_to_final(self, conn) -> None:
        """
        Why transformation is staged:
        Separating raw load from typed transformation keeps ingestion fast while
        making type enforcement explicit and auditable.
        """
        self.logger.info("Transforming staging rows into typed analytics table.")
        start = time.time()

        with conn.cursor() as cur:
            cur.execute(TRANSFORM_SQL)

        conn.commit()
        self.stats.transform_seconds = round(time.time() - start, 3)
        self.logger.info("Typed transformation completed in %.3f seconds.", self.stats.transform_seconds)

    def collect_counts(self, conn) -> None:
        """
        Why reconciliation is necessary:
        ETL quality is incomplete without verifying how many rows landed in staging
        and how many survived into the final analytical table.
        """
        with conn.cursor() as cur:
            cur.execute(RAW_COUNT_SQL)
            self.stats.raw_row_count = cur.fetchone()[0]

            cur.execute(FINAL_COUNT_SQL)
            self.stats.final_row_count = cur.fetchone()[0]

        self.logger.info("Raw staging row count: %d", self.stats.raw_row_count)
        self.logger.info("Final analytics row count: %d", self.stats.final_row_count)

    def verify_datetime_types(self, conn) -> None:
        """
        Why this check is explicit:
        The project requirement states datetime columns must be TIMESTAMP, not
        TEXT. This verification prevents silent schema drift.
        """
        with conn.cursor() as cur:
            cur.execute(DATATYPE_CHECK_SQL)
            rows = cur.fetchall()

        if len(rows) != 2:
            raise RuntimeError("Datetime column verification returned incomplete results.")

        for column_name, data_type in rows:
            self.logger.info("Column %s type: %s", column_name, data_type)
            if data_type != "timestamp without time zone":
                raise TypeError(
                    f"Column {column_name} is {data_type}; expected timestamp without time zone."
                )

    def run(self) -> Dict[str, Any]:
        """
        Why this method orchestrates the ETL:
        A single deterministic control path makes the ETL reproducible and easier
        to audit before moving to indexing benchmarks.
        """
        self.stats.started_at_epoch = time.time()
        self.validate_csv()

        conn = None
        try:
            conn = self.connect()
            self.logger.info("Connected to PostgreSQL database: %s", self.db_config["dbname"])

            self.create_schema(conn)
            self.copy_to_staging(conn)
            self.transform_to_final(conn)
            self.collect_counts(conn)
            self.verify_datetime_types(conn)

            self.stats.ended_at_epoch = time.time()

            summary = {
                **asdict(self.stats),
                "duration_seconds": self.stats.duration_seconds,
                "csv_file": str(self.csv_file),
                "datetime_columns_cast_to_timestamp": True
            }

            self.logger.info("Phase 2 completed successfully.")
            self.logger.info("ETL summary: %s", summary)
            return summary

        except Exception as exc:
            if conn is not None:
                conn.rollback()
            self.logger.exception("Phase 2 failed.")
            raise RuntimeError("Phase 2 ETL failed. Inspect the log output.") from exc

        finally:
            if conn is not None:
                conn.close()
                self.logger.info("Database connection closed.")


def main() -> None:
    """
    Why main is separated:
    This keeps the ETL script directly executable while remaining safe to import
    into future orchestration or testing code.
    """
    logger = configure_logging()
    etl = PostgresTaxiETL(DB_CONFIG, CSV_FILE, logger)
    summary = etl.run()

    print("\nPhase 2 completed successfully.")
    print(f"CSV source: {summary['csv_file']}")
    print(f"Raw staging rows: {summary['raw_row_count']}")
    print(f"Final analytics rows: {summary['final_row_count']}")
    print(f"Datetime cast verified: {summary['datetime_columns_cast_to_timestamp']}")
    print(f"Total runtime (seconds): {summary['duration_seconds']}")
    print(f"Log file: {LOG_FILE}")


if __name__ == "__main__":
    main()