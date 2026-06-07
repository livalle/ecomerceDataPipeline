"""
E-Commerce Data Pipeline — Cleaning & Standardisation Layer (Bronze → Silver)
==============================================================================
Handles deduplication, null imputation, type casting, outlier detection, and
data-quality validation with quarantine routing for invalid records.

Incident background: source data arrived with duplicate order IDs, mixed
date formats (ISO 8601 and dd/mm/yyyy), revenue stored as formatted strings
("R$ 1.290,50"), and ~8% null customer_id. This module documents and resolves
each issue explicitly.
"""

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger("cleaning")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
)


# ── Configuration ─────────────────────────────────────────────────────────────
@dataclass
class CleaningConfig:
    bronze_path: Path = Path("data/bronze")
    silver_path: Path = Path("data/silver")
    quarantine_path: Path = Path("data/quarantine")

    # Orders-specific thresholds (calibrated from 6-month historical data)
    min_revenue: float = 0.01          # R$ 0.01 minimum viable order
    max_revenue: float = 500_000.0     # R$ 500k ceiling — flag for manual review
    max_discount_pct: float = 100.0    # 100% discount is valid (promo campaigns)

    dedup_keys: dict[str, list[str]] = field(default_factory=lambda: {
        "orders":       ["order_id"],
        "customers":    ["customer_id"],
        "products":     ["product_id", "sku"],
        "transactions": ["transaction_id"],
    })

    null_strategy: dict[str, str] = field(default_factory=lambda: {
        # column_name → "drop" | "fill_zero" | "fill_unknown" | "fill_median"
        "customer_id":    "fill_unknown",   # Incident: 8% null — guest checkouts
        "shipping_fee":   "fill_zero",      # Free-shipping promos arrive as null
        "discount_pct":   "fill_zero",
        "product_category": "fill_unknown",
    })


# ── Data Quality Report ────────────────────────────────────────────────────────
@dataclass
class DQReport:
    entity: str
    run_ts: str
    total_raw: int = 0
    duplicates_removed: int = 0
    nulls_imputed: int = 0
    type_errors_fixed: int = 0
    outliers_quarantined: int = 0
    schema_violations: int = 0
    total_silver: int = 0

    def pass_rate(self) -> float:
        if self.total_raw == 0:
            return 0.0
        return round(self.total_silver / self.total_raw * 100, 2)

    def to_dict(self) -> dict:
        return {**self.__dict__, "pass_rate_pct": self.pass_rate()}


# ── Type Parsers ──────────────────────────────────────────────────────────────
def parse_monetary(value: Any) -> float | None:
    """
    Incident fix: revenue arrived as 'R$ 1.290,50' (Brazilian formatted string).
    Strips currency symbols and converts BR locale decimal separators.
    """
    if pd.isna(value) or value is None:
        return None
    raw = str(value).strip()
    # Remove currency symbol and surrounding whitespace
    raw = re.sub(r"[R$\s]", "", raw)
    # BR locale: dots are thousands separators, comma is decimal
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")
    try:
        result = float(raw)
        return result if result >= 0 else None
    except ValueError:
        return None


_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",     # ISO 8601 UTC
    "%Y-%m-%dT%H:%M:%S",      # ISO 8601 without timezone
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",      # Incident: legacy export format
    "%d/%m/%Y",
]


def parse_date(value: Any) -> datetime | None:
    """
    Incident fix: source API switched date format mid-year during a backend
    migration (ISO → dd/mm/yyyy). Parse defensively and normalise to UTC.
    """
    if pd.isna(value) or value is None:
        return None
    raw = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except ValueError:
            continue
    logger.debug("Unparseable date: %r", raw)
    return None


# ── Orders Cleaner ────────────────────────────────────────────────────────────
class OrdersCleaner:
    """
    Cleans and validates the /orders entity.

    Known issues addressed (documented in README incident log):
    1. Duplicate order_id — source system emits duplicate webhooks on retries
    2. Revenue stored as formatted BR locale strings
    3. Mixed date formats (ISO / dd/mm/yyyy)
    4. Null customer_id for guest checkouts (8% of rows)
    5. Negative revenue values from partial-refund accounting errors
    6. Revenue outliers > R$500k (data entry errors, not real orders)
    """

    def __init__(self, config: CleaningConfig) -> None:
        self.config = config
        self.quarantine_path = config.quarantine_path / "orders"
        self.quarantine_path.mkdir(parents=True, exist_ok=True)

    def _load_bronze(self, source_path: Path) -> pd.DataFrame:
        """Read NDJSON, unwrap metadata envelope, return flat DataFrame."""
        records = []
        with source_path.open() as f:
            for line in f:
                envelope = json.loads(line)
                row = {**envelope["payload"], "_load_ts": envelope["_meta"]["load_ts"]}
                records.append(row)
        df = pd.DataFrame(records)
        logger.info("Loaded %d rows from %s", len(df), source_path.name)
        return df

    def _deduplicate(self, df: pd.DataFrame, report: DQReport) -> pd.DataFrame:
        """Keep the most recent record per order_id (last-write-wins)."""
        before = len(df)
        df = df.sort_values("_load_ts", ascending=False).drop_duplicates(
            subset=["order_id"], keep="first"
        )
        report.duplicates_removed = before - len(df)
        if report.duplicates_removed:
            logger.info("Removed %d duplicate order IDs", report.duplicates_removed)
        return df

    def _fix_types(self, df: pd.DataFrame, report: DQReport) -> pd.DataFrame:
        """Cast columns to target schema. Count and log conversion failures."""
        errors = 0

        # Revenue
        original_revenue = df["revenue"].copy()
        df["revenue"] = df["revenue"].apply(parse_monetary)
        failed = df["revenue"].isna() & original_revenue.notna()
        errors += failed.sum()
        if failed.sum():
            logger.warning("%d revenue values could not be parsed", failed.sum())

        # Dates
        for col in ["order_date", "shipped_date", "delivered_date"]:
            if col in df.columns:
                df[col] = df[col].apply(parse_date)

        # Integers
        for col in ["quantity", "item_count"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        report.type_errors_fixed = errors
        return df

    def _impute_nulls(self, df: pd.DataFrame, report: DQReport) -> pd.DataFrame:
        """Apply per-column null strategies from config."""
        imputed = 0
        for col, strategy in self.config.null_strategy.items():
            if col not in df.columns:
                continue
            null_mask = df[col].isna()
            count = null_mask.sum()
            if count == 0:
                continue

            if strategy == "fill_unknown":
                df.loc[null_mask, col] = "UNKNOWN"
            elif strategy == "fill_zero":
                df.loc[null_mask, col] = 0
            elif strategy == "fill_median":
                median = df[col].median()
                df.loc[null_mask, col] = median
            elif strategy == "drop":
                df = df.dropna(subset=[col])

            imputed += count
            logger.info("Imputed %d nulls in '%s' (strategy: %s)", count, col, strategy)

        report.nulls_imputed = imputed
        return df

    def _quarantine_outliers(self, df: pd.DataFrame, report: DQReport) -> pd.DataFrame:
        """
        Separate outliers into quarantine. They are preserved — not deleted —
        so domain experts can inspect and potentially reclassify.

        Incident note: early versions hard-deleted outliers. Three valid bulk
        corporate orders (R$280k each) were permanently lost. Quarantine
        was introduced in v0.3 after this incident.
        """
        cfg = self.config

        bad_mask = (
            (df["revenue"] < cfg.min_revenue) |
            (df["revenue"] > cfg.max_revenue) |
            (df["revenue"].isna())
        )

        if "discount_pct" in df.columns:
            bad_mask |= df["discount_pct"] > cfg.max_discount_pct

        quarantine_df = df[bad_mask].copy()
        quarantine_df["_quarantine_reason"] = (
            quarantine_df["revenue"].apply(
                lambda v: "revenue_null" if pd.isna(v)
                else "revenue_too_low" if v < cfg.min_revenue
                else "revenue_too_high"
            )
        )

        if not quarantine_df.empty:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            out = self.quarantine_path / f"orders_quarantine_{ts}.ndjson"
            quarantine_df.to_json(out, orient="records", lines=True, date_format="iso")
            logger.warning(
                "Quarantined %d records → %s (%.1f%% of load)",
                len(quarantine_df),
                out,
                len(quarantine_df) / len(df) * 100,
            )

        report.outliers_quarantined = bad_mask.sum()
        return df[~bad_mask].copy()

    def _add_silver_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Append audit columns for lineage tracking."""
        df["_silver_ts"] = datetime.now(timezone.utc).isoformat()
        df["_pipeline_version"] = "1.0.0"
        return df

    def clean(self, source_path: Path) -> tuple[Path, DQReport]:
        """
        Full cleaning pipeline. Returns (output_path, DQReport).
        Raises on unrecoverable schema violations.
        """
        report = DQReport(
            entity="orders",
            run_ts=datetime.now(timezone.utc).isoformat(),
        )

        df = self._load_bronze(source_path)
        report.total_raw = len(df)

        required_columns = {"order_id", "revenue", "order_date"}
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"Schema violation: missing required columns {missing}")

        df = self._deduplicate(df, report)
        df = self._fix_types(df, report)
        df = self._impute_nulls(df, report)
        df = self._quarantine_outliers(df, report)
        df = self._add_silver_metadata(df)

        report.total_silver = len(df)

        # Persist to Silver
        output_dir = self.config.silver_path / "orders" / source_path.parts[-2]
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / source_path.name.replace(".ndjson", "_silver.parquet")
        df.to_parquet(output_path, index=False, compression="snappy")

        logger.info(
            "Silver write complete: %d rows (pass rate %.1f%%) → %s",
            report.total_silver,
            report.pass_rate(),
            output_path,
        )
        return output_path, report


# ── Great Expectations Checkpoint ────────────────────────────────────────────
def run_data_quality_checks(df: pd.DataFrame, entity: str) -> dict[str, bool]:
    """
    Lightweight expectation suite — no GE install required for CI.
    Replace with a full ge.DataContext checkpoint in production.

    Thresholds derived from 6-month historical analysis of the orders dataset.
    """
    results: dict[str, bool] = {}

    if entity == "orders":
        results["revenue_non_negative"] = bool((df["revenue"] >= 0).all())
        results["order_id_unique"] = bool(df["order_id"].nunique() == len(df))
        results["order_date_not_null"] = bool(df["order_date"].notna().all())
        results["revenue_not_null"] = bool(df["revenue"].notna().all())
        results["completeness_above_90pct"] = bool(
            df.notna().mean().mean() >= 0.90
        )
        if "discount_pct" in df.columns:
            results["discount_in_range"] = bool(
                df["discount_pct"].between(0, 100).all()
            )

    failed = [k for k, v in results.items() if not v]
    if failed:
        logger.error("DQ FAILED checks: %s", failed)
    else:
        logger.info("All DQ checks passed for entity '%s'", entity)

    return results


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    config = CleaningConfig()
    cleaner = OrdersCleaner(config)

    # Process all bronze files for orders
    bronze_files = sorted(Path("data/bronze/orders").rglob("*.ndjson"))
    all_reports = []
    for bronze_file in bronze_files:
        silver_path, report = cleaner.clean(bronze_file)
        all_reports.append(report.to_dict())

    print(json.dumps(all_reports, indent=2, default=str))
