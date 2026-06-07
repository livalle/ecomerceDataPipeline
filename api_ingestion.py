"""
E-Commerce Data Pipeline — Ingestion Layer
==========================================
Handles API ingestion with rate limiting, exponential backoff, and DLQ routing.

Incident background: upstream e-commerce API returned HTTP 429 under burst load
and silently dropped records during peak hours. This module implements defensive
ingestion patterns to prevent silent data loss.
"""

import json
import logging
import time
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator
from collections import deque

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("ingestion")


# ── Configuration ─────────────────────────────────────────────────────────────
@dataclass
class IngestionConfig:
    """Centralised configuration. Externalise to env vars or a secrets manager
    before promoting to production."""

    base_url: str = "https://api.example-ecommerce.io/v2"
    api_key: str = "YOUR_API_KEY"

    # Rate-limit budget: keep well below provider's hard ceiling
    requests_per_second: float = 2.0   # Provider ceiling: 10 rps — we use 20%
    max_burst: int = 5                  # Leaky-bucket token ceiling

    # Retry / backoff
    max_retries: int = 5
    backoff_base: float = 1.0          # seconds; doubled each attempt
    backoff_max: float = 60.0
    backoff_jitter: float = 0.3        # ±30% random jitter to avoid thundering herd

    # Storage
    raw_data_path: Path = Path("data/bronze")
    dlq_path: Path = Path("data/dlq")
    batch_size: int = 100

    # Endpoints to ingest
    endpoints: list[str] = field(default_factory=lambda: [
        "/orders",
        "/products",
        "/customers",
        "/transactions",
    ])


# ── Rate Limiter (token bucket) ───────────────────────────────────────────────
class TokenBucketRateLimiter:
    """
    Token bucket algorithm for smooth, burst-tolerant rate limiting.

    Incident root cause: a simple sleep-based limiter caused all threads to
    wake simultaneously after a 429, recreating the burst that caused the
    429 in the first place.

    Fix: token bucket with per-request jitter decouples request timing.
    """

    def __init__(self, rate: float, max_burst: int) -> None:
        self.rate = rate          # tokens added per second
        self.max_burst = max_burst
        self._tokens: float = max_burst
        self._last_refill: float = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.max_burst, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def acquire(self, tokens: int = 1) -> None:
        """Block until enough tokens are available."""
        while True:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return
            # Sleep for roughly the time needed to accumulate one token
            deficit = tokens - self._tokens
            sleep_time = deficit / self.rate
            jitter = random.uniform(-0.05, 0.05) * sleep_time
            time.sleep(max(0.0, sleep_time + jitter))


# ── Dead Letter Queue ─────────────────────────────────────────────────────────
class DeadLetterQueue:
    """
    Persists failed requests for later reprocessing or alerting.

    Incident context: during the first outage, ~3 400 records were silently
    dropped because failures weren't captured. The DLQ now provides a
    complete audit trail and enables targeted replay.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)
        self._buffer: deque = deque()

    def push(self, endpoint: str, params: dict, error: str, status_code: int | None) -> None:
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "endpoint": endpoint,
            "params": params,
            "error": error,
            "status_code": status_code,
        }
        self._buffer.append(record)
        logger.warning("DLQ ← %s | status=%s | %s", endpoint, status_code, error)

    def flush(self) -> None:
        """Write buffered failures to NDJSON for downstream inspection."""
        if not self._buffer:
            return
        filename = self.path / f"dlq_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.ndjson"
        with filename.open("w") as f:
            while self._buffer:
                f.write(json.dumps(self._buffer.popleft()) + "\n")
        logger.info("DLQ flushed → %s", filename)


# ── HTTP Session ──────────────────────────────────────────────────────────────
def _build_session(config: IngestionConfig) -> requests.Session:
    """
    Requests session with connection-level retries (network errors only).
    Application-level 429/5xx retries are handled by ApiIngestionClient
    to enable DLQ routing and custom backoff.
    """
    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET"],
        )
    )
    session.mount("https://", adapter)
    session.headers.update({
        "Authorization": f"Bearer {config.api_key}",
        "Accept": "application/json",
        "User-Agent": "ecommerce-pipeline/1.0",
    })
    return session


# ── Ingestion Client ──────────────────────────────────────────────────────────
class ApiIngestionClient:
    """
    Fetches paginated data from the e-commerce API with rate limiting,
    exponential backoff, and DLQ routing for unrecoverable failures.
    """

    def __init__(self, config: IngestionConfig) -> None:
        self.config = config
        self.session = _build_session(config)
        self.limiter = TokenBucketRateLimiter(
            rate=config.requests_per_second,
            max_burst=config.max_burst,
        )
        self.dlq = DeadLetterQueue(config.dlq_path)
        self._stats = {"success": 0, "retried": 0, "dlq": 0, "records": 0}

    def _backoff_sleep(self, attempt: int) -> None:
        delay = min(
            self.config.backoff_base * (2 ** attempt),
            self.config.backoff_max,
        )
        jitter = delay * self.config.backoff_jitter * random.uniform(-1, 1)
        actual = max(0.0, delay + jitter)
        logger.info("Backoff attempt %d — sleeping %.2fs", attempt + 1, actual)
        time.sleep(actual)

    def _get(self, endpoint: str, params: dict) -> dict | None:
        """Single request with retry loop. Returns None on terminal failure."""
        url = f"{self.config.base_url}{endpoint}"
        for attempt in range(self.config.max_retries + 1):
            self.limiter.acquire()
            try:
                resp = self.session.get(url, params=params, timeout=10)

                if resp.status_code == 200:
                    self._stats["success"] += 1
                    return resp.json()

                if resp.status_code == 429:
                    # Honour Retry-After header when present
                    retry_after = int(resp.headers.get("Retry-After", 0))
                    if retry_after:
                        logger.warning("Rate-limited. Honouring Retry-After: %ds", retry_after)
                        time.sleep(retry_after)
                    else:
                        self._backoff_sleep(attempt)
                    self._stats["retried"] += 1
                    continue

                if resp.status_code in (500, 502, 503, 504) and attempt < self.config.max_retries:
                    self._backoff_sleep(attempt)
                    self._stats["retried"] += 1
                    continue

                # Non-retryable (4xx client errors)
                self.dlq.push(endpoint, params, f"HTTP {resp.status_code}", resp.status_code)
                self._stats["dlq"] += 1
                return None

            except requests.exceptions.Timeout:
                if attempt < self.config.max_retries:
                    self._backoff_sleep(attempt)
                    self._stats["retried"] += 1
                else:
                    self.dlq.push(endpoint, params, "Timeout after max retries", None)
                    self._stats["dlq"] += 1
                    return None

            except requests.exceptions.RequestException as exc:
                self.dlq.push(endpoint, params, str(exc), None)
                self._stats["dlq"] += 1
                return None

        self.dlq.push(endpoint, params, "Exhausted all retries", None)
        self._stats["dlq"] += 1
        return None

    def paginate(self, endpoint: str, date_from: str, date_to: str) -> Generator[list[dict], None, None]:
        """Yields pages of records. Handles cursor-based pagination."""
        cursor = None
        page = 1
        while True:
            params: dict[str, Any] = {
                "date_from": date_from,
                "date_to": date_to,
                "limit": self.config.batch_size,
            }
            if cursor:
                params["cursor"] = cursor

            logger.info("Fetching %s page %d (cursor=%s)", endpoint, page, cursor)
            data = self._get(endpoint, params)

            if data is None:
                logger.error("Aborting pagination for %s at page %d", endpoint, page)
                return

            records = data.get("data", [])
            if not records:
                break

            self._stats["records"] += len(records)
            yield records

            cursor = data.get("next_cursor")
            if not cursor:
                break
            page += 1

    def fetch_and_persist(self, endpoint: str, date_from: str, date_to: str) -> Path:
        """
        Full pipeline: paginate → tag with metadata → write to Bronze layer.

        Each file is self-describing: it carries load timestamp, source endpoint,
        and date range so downstream jobs can reconstruct lineage without a
        separate catalog.
        """
        output_dir = self.config.raw_data_path / endpoint.strip("/") / date_from[:7]
        output_dir.mkdir(parents=True, exist_ok=True)

        load_ts = datetime.now(timezone.utc).isoformat()
        filename = output_dir / f"{endpoint.strip('/').replace('/', '_')}_{date_from}_{date_to}.ndjson"

        record_count = 0
        with filename.open("w") as f:
            for page in self.paginate(endpoint, date_from, date_to):
                for record in page:
                    envelope = {
                        "_meta": {
                            "source": endpoint,
                            "load_ts": load_ts,
                            "date_from": date_from,
                            "date_to": date_to,
                        },
                        "payload": record,
                    }
                    f.write(json.dumps(envelope) + "\n")
                    record_count += 1

        logger.info("Persisted %d records → %s", record_count, filename)
        self.dlq.flush()
        return filename

    def run_full_load(self, date_from: str, date_to: str) -> dict:
        """Orchestrate ingestion for all configured endpoints."""
        logger.info("Starting full load | %s → %s", date_from, date_to)
        results = {}
        for endpoint in self.config.endpoints:
            try:
                path = self.fetch_and_persist(endpoint, date_from, date_to)
                results[endpoint] = {"status": "ok", "path": str(path)}
            except Exception as exc:
                logger.exception("Unexpected failure on %s: %s", endpoint, exc)
                results[endpoint] = {"status": "error", "error": str(exc)}

        logger.info("Load complete. Stats: %s", self._stats)
        return {"stats": self._stats, "results": results}


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    config = IngestionConfig()
    client = ApiIngestionClient(config)
    summary = client.run_full_load(
        date_from="2024-01-01",
        date_to="2024-01-31",
    )
    print(json.dumps(summary, indent=2, default=str))
