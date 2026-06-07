"""
Unit tests — Ingestion & Cleaning layers
=========================================
Run with: pytest tests/ -v --tb=short
"""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

# ── Token Bucket ──────────────────────────────────────────────────────────────
class TestTokenBucketRateLimiter:
    def _make(self, rate=10.0, burst=5):
        from ingestion.api_ingestion import TokenBucketRateLimiter
        return TokenBucketRateLimiter(rate=rate, max_burst=burst)

    def test_acquire_within_burst_is_fast(self):
        limiter = self._make(rate=10, burst=5)
        start = time.monotonic()
        for _ in range(5):
            limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.2, f"Expected < 0.2s for burst, got {elapsed:.3f}s"

    def test_tokens_cannot_exceed_max_burst(self):
        limiter = self._make(rate=1, burst=3)
        time.sleep(5)  # let bucket overflow
        limiter._refill()
        assert limiter._tokens <= 3


# ── Monetary Parser ────────────────────────────────────────────────────────────
class TestParseMonetary:
    def _p(self, v):
        from cleaning.data_cleaning import parse_monetary
        return parse_monetary(v)

    def test_plain_float_string(self):
        assert self._p("1290.50") == pytest.approx(1290.50)

    def test_br_locale_format(self):
        assert self._p("R$ 1.290,50") == pytest.approx(1290.50)

    def test_br_locale_no_symbol(self):
        assert self._p("1.290,50") == pytest.approx(1290.50)

    def test_integer_string(self):
        assert self._p("500") == pytest.approx(500.0)

    def test_negative_returns_none(self):
        assert self._p("-100") is None

    def test_none_input(self):
        assert self._p(None) is None

    def test_nan_input(self):
        assert self._p(float("nan")) is None

    def test_empty_string(self):
        assert self._p("") is None

    def test_non_numeric_string(self):
        assert self._p("N/A") is None


# ── Date Parser ────────────────────────────────────────────────────────────────
class TestParseDate:
    def _p(self, v):
        from cleaning.data_cleaning import parse_date
        return parse_date(v)

    def test_iso_8601(self):
        result = self._p("2024-01-15T10:30:00Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1

    def test_br_format(self):
        result = self._p("15/01/2024")
        assert result is not None
        assert result.day == 15

    def test_none_returns_none(self):
        assert self._p(None) is None

    def test_unparseable_returns_none(self):
        assert self._p("not-a-date") is None


# ── Orders Cleaner ─────────────────────────────────────────────────────────────
@pytest.fixture
def sample_bronze_file(tmp_path):
    """Create a minimal Bronze NDJSON file for testing."""
    data = [
        {"order_id": "O001", "revenue": "R$ 1.500,00", "order_date": "15/01/2024",
         "customer_id": None, "discount_pct": None, "shipping_fee": None, "quantity": 2},
        {"order_id": "O002", "revenue": "250.00",     "order_date": "2024-01-16T09:00:00Z",
         "customer_id": "C99", "discount_pct": "10", "shipping_fee": "15.00", "quantity": 1},
        {"order_id": "O001", "revenue": "R$ 1.500,00","order_date": "15/01/2024",
         "customer_id": None, "discount_pct": None, "shipping_fee": None, "quantity": 2},  # dup
        {"order_id": "O003", "revenue": "-50.00",     "order_date": "2024-01-17",
         "customer_id": "C01", "discount_pct": "0",  "shipping_fee": "0",   "quantity": 1},  # negative
    ]
    bronze_dir = tmp_path / "bronze" / "orders" / "2024-01"
    bronze_dir.mkdir(parents=True)
    filepath = bronze_dir / "orders_2024-01-01_2024-01-31.ndjson"
    with filepath.open("w") as f:
        for row in data:
            envelope = {"_meta": {"source": "/orders", "load_ts": "2024-01-31T06:00:00Z",
                                   "date_from": "2024-01-01", "date_to": "2024-01-31"},
                        "payload": row}
            f.write(json.dumps(envelope) + "\n")
    return filepath


class TestOrdersCleaner:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path, sample_bronze_file):
        from cleaning.data_cleaning import CleaningConfig, OrdersCleaner
        self.config = CleaningConfig(
            bronze_path=tmp_path / "bronze",
            silver_path=tmp_path / "silver",
            quarantine_path=tmp_path / "quarantine",
        )
        self.cleaner = OrdersCleaner(self.config)
        self.source_path = sample_bronze_file

    def test_deduplication_removes_duplicate_order_id(self):
        silver_path, report = self.cleaner.clean(self.source_path)
        assert report.duplicates_removed == 1

    def test_negative_revenue_quarantined(self):
        silver_path, report = self.cleaner.clean(self.source_path)
        assert report.outliers_quarantined >= 1

    def test_null_customer_id_imputed(self):
        silver_path, report = self.cleaner.clean(self.source_path)
        assert report.nulls_imputed >= 1

    def test_silver_parquet_created(self):
        silver_path, _ = self.cleaner.clean(self.source_path)
        assert silver_path.exists()
        df = pd.read_parquet(silver_path)
        assert len(df) > 0

    def test_silver_has_metadata_columns(self):
        silver_path, _ = self.cleaner.clean(self.source_path)
        df = pd.read_parquet(silver_path)
        assert "_silver_ts" in df.columns
        assert "_pipeline_version" in df.columns

    def test_revenue_all_positive_in_silver(self):
        silver_path, _ = self.cleaner.clean(self.source_path)
        df = pd.read_parquet(silver_path)
        assert (df["revenue"] > 0).all()


# ── DLQ ────────────────────────────────────────────────────────────────────────
class TestDeadLetterQueue:
    def test_flush_writes_ndjson(self, tmp_path):
        from ingestion.api_ingestion import DeadLetterQueue
        dlq = DeadLetterQueue(tmp_path)
        dlq.push("/orders", {"limit": 100}, "HTTP 400", 400)
        dlq.push("/orders", {"limit": 100}, "Timeout", None)
        dlq.flush()
        files = list(tmp_path.glob("dlq_*.ndjson"))
        assert len(files) == 1
        lines = files[0].read_text().strip().split("\n")
        assert len(lines) == 2
        record = json.loads(lines[0])
        assert record["endpoint"] == "/orders"
        assert record["status_code"] == 400

    def test_flush_empty_queue_creates_no_file(self, tmp_path):
        from ingestion.api_ingestion import DeadLetterQueue
        dlq = DeadLetterQueue(tmp_path)
        dlq.flush()
        assert list(tmp_path.glob("dlq_*.ndjson")) == []


# ── DQ Checks ─────────────────────────────────────────────────────────────────
class TestDataQualityChecks:
    def test_all_pass_on_clean_data(self):
        from cleaning.data_cleaning import run_data_quality_checks
        df = pd.DataFrame({
            "order_id": ["O1", "O2", "O3"],
            "revenue":  [100.0, 200.0, 300.0],
            "order_date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "discount_pct": [0, 10, 5],
        })
        results = run_data_quality_checks(df, "orders")
        assert all(results.values()), f"Failed: {[k for k,v in results.items() if not v]}"

    def test_fails_on_duplicate_order_id(self):
        from cleaning.data_cleaning import run_data_quality_checks
        df = pd.DataFrame({
            "order_id": ["O1", "O1"],
            "revenue":  [100.0, 200.0],
            "order_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "discount_pct": [0, 0],
        })
        results = run_data_quality_checks(df, "orders")
        assert results["order_id_unique"] is False

    def test_fails_on_negative_revenue(self):
        from cleaning.data_cleaning import run_data_quality_checks
        df = pd.DataFrame({
            "order_id": ["O1"],
            "revenue":  [-50.0],
            "order_date": pd.to_datetime(["2024-01-01"]),
        })
        results = run_data_quality_checks(df, "orders")
        assert results["revenue_non_negative"] is False
