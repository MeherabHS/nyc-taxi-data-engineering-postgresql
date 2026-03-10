"""
phase1_parquet_to_clean_csv.py

Why this file exists:
The project requires a memory-aware Phase 1 ingestion and cleaning pipeline for a
large-scale dataset. The source data is available as monthly Parquet files, but
the deliverable for this phase is a consolidated cleaned CSV suitable for later
ETL and SQL loading.

Why this design was selected:
Creating a giant raw CSV first and then re-reading it for cleaning is inefficient.
This script performs conversion and cleaning in a single streaming pipeline:
Parquet batch -> clean -> append to final CSV.

Project scope alignment:
This file implements only Phase 1. It does not define database schema, indexing,
or SQL benchmarks. Its responsibility is to produce a clean, consistent dataset
artifact and a processing report for the next phase.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # Defensive import guard for a required dependency.
    raise ImportError(
        "pyarrow is required for Parquet batch ingestion. "
        "Install it with: pip install pyarrow"
    ) from exc


# -----------------------------------------------------------------------------
# Configuration
# Why:
# These settings isolate environment-specific paths and processing parameters so
# the pipeline can be adapted without changing core logic. This keeps the script
# portable and reduces the risk of hidden hardcoded assumptions.
# -----------------------------------------------------------------------------
INPUT_DIR = Path(r"C:\Users\Asus\OneDrive\Desktop\taxi_data")
OUTPUT_DIR = Path(r"C:\Users\Asus\OneDrive\Desktop\taxi_data\taxi_project_output")

OUTPUT_CSV = OUTPUT_DIR / "yellow_taxi_2024_cleaned.csv"
SUMMARY_JSON = OUTPUT_DIR / "phase1_processing_summary.json"
LOG_FILE = OUTPUT_DIR / "phase1_processing.log"

# Why this batch size:
# Batch processing bounds memory usage while still keeping throughput reasonable.
# It can be tuned later based on the user's machine constraints and observed logs.
BATCH_SIZE = 100_000

# Why these fields:
# These are the minimum fields needed for a structurally valid trip record and
# are strong candidates for future relational design and SQL workload analysis.
CRITICAL_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
]

NUMERIC_COLUMNS = [
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

DATETIME_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

TEXT_COLUMNS = [
    "store_and_fwd_flag",
]

NEGATIVE_NOT_ALLOWED = [
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "tip_amount",
    "tolls_amount",
]


# -----------------------------------------------------------------------------
# Runtime logging
# Why:
# Large-file pipelines must be observable. Without logs, debugging ingestion
# failures or explaining performance behavior in later phases becomes guesswork.
# -----------------------------------------------------------------------------
def configure_logging(log_file: Path) -> logging.Logger:
    """
    Why this function exists:
    Logging is configured after output directory creation, ensuring both console
    and file logging are available for the entire pipeline lifecycle.
    """
    logger = logging.getLogger("phase1")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


@dataclass
class ProcessingStats:
    """
    Why stats are formalized:
    Phase 1 should end with measurable evidence. This structure captures the
    ingestion footprint and cleaning impact for handoff into ETL planning.
    """
    files_discovered: int = 0
    files_processed: int = 0
    batches_processed: int = 0
    rows_read: int = 0
    rows_written: int = 0
    rows_dropped: int = 0
    started_at_epoch: float = 0.0
    ended_at_epoch: float = 0.0

    @property
    def duration_seconds(self) -> float:
        """
        Why duration is derived:
        Duration should always reflect the real runtime boundaries instead of
        being manually maintained and potentially drifting from reality.
        """
        return round(self.ended_at_epoch - self.started_at_epoch, 3)


class YellowTaxiPhase1Processor:
    """
    Why a class is used:
    The pipeline has a coherent lifecycle: validation, discovery, batch cleaning,
    writing, and summary generation. Encapsulating that lifecycle improves
    maintainability without introducing unnecessary framework complexity.
    """

    def __init__(
        self,
        input_dir: Path,
        output_csv: Path,
        summary_json: Path,
        batch_size: int,
        logger: logging.Logger,
    ) -> None:
        """
        Why dependencies are injected:
        Explicit dependency injection keeps the processing logic testable and makes
        environment-specific configuration independent from the implementation.
        """
        self.input_dir = input_dir
        self.output_csv = output_csv
        self.summary_json = summary_json
        self.batch_size = batch_size
        self.logger = logger
        self.stats = ProcessingStats()

    def validate_environment(self) -> None:
        """
        Why environment validation runs first:
        The pipeline should fail fast on missing input directories or inaccessible
        output locations rather than failing mid-ingestion after partial writes.
        """
        if not self.input_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {self.input_dir}")

        if not self.input_dir.is_dir():
            raise NotADirectoryError(f"Input path is not a directory: {self.input_dir}")

        self.output_csv.parent.mkdir(parents=True, exist_ok=True)

        self.logger.info("Validated input directory: %s", self.input_dir)
        self.logger.info("Validated output directory: %s", self.output_csv.parent)

    def discover_parquet_files(self) -> List[Path]:
        """
        Why deterministic discovery matters:
        A stable file ordering makes pipeline behavior reproducible, which is
        necessary when comparing logs, row counts, or rerunning the process.
        """
        files = sorted(self.input_dir.glob("yellow_tripdata_2024-*.parquet"))
        self.stats.files_discovered = len(files)

        if not files:
            raise FileNotFoundError(
                "No files matched pattern 'yellow_tripdata_2024-*.parquet' "
                f"in directory: {self.input_dir}"
            )

        self.logger.info("Discovered %d parquet files.", len(files))
        for file in files:
            self.logger.info("Queued file: %s", file.name)

        return files

    @staticmethod
    def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Why column normalization is conservative:
        Header inconsistencies cause downstream schema drift. Normalizing here
        keeps future ETL and SQL definitions simpler and more deterministic.
        """
        df.columns = [str(col).strip().lower() for col in df.columns]
        return df

    @staticmethod
    def trim_text_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Why text trimming is limited:
        Phase 1 should correct low-risk formatting defects without making domain
        assumptions that belong in later analytical stages.
        """
        for col in TEXT_COLUMNS:
            if col in df.columns:
                df[col] = df[col].astype("string").str.strip()

        return df

    @staticmethod
    def convert_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Why datetime parsing is explicit:
        Timestamp fields are central to both relational modeling and SQL
        optimization. Converting them now prevents string-based ambiguity later.
        """
        for col in DATETIME_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        return df

    @staticmethod
    def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Why numeric coercion is defensive:
        Malformed numeric values should not crash the pipeline. Coercion preserves
        continuity while allowing invalid rows to be removed transparently.
        """
        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def apply_quality_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Why the quality rules are intentionally minimal:
        Phase 1 is about structural cleaning, not aggressive business filtering.
        The goal is a stable and trustworthy dataset for ETL, not early modeling.
        """
        before = len(df)

        present_critical = [col for col in CRITICAL_COLUMNS if col in df.columns]
        if present_critical:
            df = df.dropna(subset=present_critical)

        for col in NEGATIVE_NOT_ALLOWED:
            if col in df.columns:
                df = df[df[col].isna() | (df[col] >= 0)]

        df = df.drop_duplicates()

        dropped = before - len(df)
        self.stats.rows_dropped += dropped
        return df

    def downcast_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Why downcasting is applied:
        Reducing in-memory representation per batch improves throughput and
        reinforces the memory-aware objective of the phase.
        """
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], downcast="integer")
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], downcast="float")

        return df

    def clean_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Why a dedicated cleaning method exists:
        A single cleaning path guarantees that every batch is transformed with the
        same rules, which is necessary for consistency and auditability.
        """
        df = self.normalize_columns(df)
        df = self.trim_text_columns(df)
        df = self.convert_datetime_columns(df)
        df = self.convert_numeric_columns(df)
        df = self.apply_quality_rules(df)
        df = self.downcast_numeric_columns(df)
        return df

    def write_batch(self, df: pd.DataFrame, write_header: bool) -> None:
        """
        Why append-mode writing is used:
        Streaming directly to the final CSV avoids accumulating a large in-memory
        DataFrame and avoids a second full-file cleaning pass.
        """
        df.to_csv(
            self.output_csv,
            mode="w" if write_header else "a",
            header=write_header,
            index=False,
        )

    def iter_parquet_batches(self, parquet_path: Path) -> Iterable[pd.DataFrame]:
        """
        Why pyarrow batch iteration is used:
        Pandas does not provide chunked parquet reading equivalent to CSV chunking.
        Arrow batch iteration provides the bounded-memory behavior needed for
        large-file processing.
        """
        parquet_file = pq.ParquetFile(parquet_path)

        for record_batch in parquet_file.iter_batches(batch_size=self.batch_size):
            table = pa.Table.from_batches([record_batch])
            yield table.to_pandas()

    def process(self) -> None:
        """
        Why this method orchestrates the full phase:
        Phase 1 should be executed through one deterministic control path so the
        generated logs and summary report are reproducible and reviewable.
        """
        self.stats.started_at_epoch = time.time()
        self.validate_environment()

        parquet_files = self.discover_parquet_files()

        if self.output_csv.exists():
            self.logger.warning("Output CSV already exists and will be overwritten: %s", self.output_csv)
            self.output_csv.unlink()

        write_header = True

        for parquet_file in parquet_files:
            self.logger.info("Starting file: %s", parquet_file.name)

            try:
                for batch_df in self.iter_parquet_batches(parquet_file):
                    self.stats.batches_processed += 1
                    self.stats.rows_read += len(batch_df)

                    cleaned_df = self.clean_batch(batch_df)
                    self.write_batch(cleaned_df, write_header=write_header)

                    self.stats.rows_written += len(cleaned_df)
                    write_header = False

                    self.logger.info(
                        "Processed batch %d | source rows=%d | written rows=%d",
                        self.stats.batches_processed,
                        len(batch_df),
                        len(cleaned_df),
                    )

                self.stats.files_processed += 1
                self.logger.info("Completed file: %s", parquet_file.name)

            except Exception as exc:
                self.logger.exception("Processing failed for file: %s", parquet_file)
                raise RuntimeError(
                    f"Processing failed for file: {parquet_file.name}"
                ) from exc

        self.stats.ended_at_epoch = time.time()
        self.write_summary()
        self.logger.info("Phase 1 completed successfully.")
        self.logger.info("Summary JSON written to: %s", self.summary_json)

    def write_summary(self) -> None:
        """
        Why the summary is persisted to JSON:
        A machine-readable report makes the Phase 1 output easy to inspect,
        compare across reruns, and use as evidence in later project phases.
        """
        summary = {
            **asdict(self.stats),
            "duration_seconds": self.stats.duration_seconds,
            "output_csv": str(self.output_csv),
            "output_csv_exists": self.output_csv.exists(),
            "output_csv_size_mb": round(
                self.output_csv.stat().st_size / (1024 * 1024), 3
            ) if self.output_csv.exists() else None,
        }

        with self.summary_json.open("w", encoding="utf-8") as file:
            json.dump(summary, file, indent=2)


def main() -> None:
    """
    Why main exists:
    The main guard keeps the module import-safe for future testing or orchestration
    while still providing direct script execution for the current phase.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger = configure_logging(LOG_FILE)

    processor = YellowTaxiPhase1Processor(
        input_dir=INPUT_DIR,
        output_csv=OUTPUT_CSV,
        summary_json=SUMMARY_JSON,
        batch_size=BATCH_SIZE,
        logger=logger,
    )

    processor.process()

    print("\nPhase 1 completed.")
    print(f"Cleaned CSV: {OUTPUT_CSV}")
    print(f"Summary JSON: {SUMMARY_JSON}")
    print(f"Log file: {LOG_FILE}")


if __name__ == "__main__":
    main()