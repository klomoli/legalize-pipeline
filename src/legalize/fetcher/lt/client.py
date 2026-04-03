"""Lithuania data.gov.lt Spinta API client.

Single source: https://get.data.gov.lt (Spinta API, UAPI spec)
All data (metadata + full text via tekstas_lt) comes from data.gov.lt.
e-tar.lt is only used for source URLs, not for fetching.
License: Open data (Creative Commons)
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import requests

from legalize.fetcher.base import LegislativeClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://get.data.gov.lt"
DEFAULT_DATASET = "datasets/gov/lrsk/teises_aktai/Dokumentas"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_RATE_LIMIT = 2.0  # requests per second

# Fields needed for metadata
_META_FIELDS = (
    "dokumento_id,pavadinimas,alt_pavadinimas,rusis,galioj_busena,"
    "priimtas,isigalioja,negalioja,priemusi_inst,nuoroda,tar_kodas,pakeista"
)

# Fields needed for discovery
_DISCOVERY_FIELDS = "dokumento_id,rusis,galioj_busena,priimtas,pavadinimas"


class TARClient(LegislativeClient):
    """HTTP client for Lithuanian legislation via data.gov.lt Spinta API.

    Single-source: both metadata and full text (tekstas_lt field)
    come from the same API. e-tar.lt has Cloudflare protection and
    is not used for fetching.
    """

    @classmethod
    def create(cls, country_config: CountryConfig) -> TARClient:
        """Create TARClient from CountryConfig."""
        source = country_config.source or {}
        return cls(
            api_url=source.get("api_url", DEFAULT_API_URL),
            dataset=source.get("dataset", DEFAULT_DATASET),
            timeout=source.get("request_timeout", DEFAULT_TIMEOUT),
            max_retries=source.get("max_retries", DEFAULT_MAX_RETRIES),
            requests_per_second=source.get("requests_per_second", DEFAULT_RATE_LIMIT),
        )

    def __init__(
        self,
        api_url: str = DEFAULT_API_URL,
        dataset: str = DEFAULT_DATASET,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        requests_per_second: float = DEFAULT_RATE_LIMIT,
    ) -> None:
        self._api_url = api_url.rstrip("/")
        self._dataset = dataset
        self._timeout = timeout
        self._max_retries = max_retries
        self._delay = 1.0 / requests_per_second
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "legalize-bot/1.0"})

    def get_text(self, norm_id: str) -> bytes:
        """Fetch full text from data.gov.lt via the tekstas_lt field.

        Args:
            norm_id: Document ID (dokumento_id), e.g. "TAR.47BB952431DA"

        Returns:
            Text content as UTF-8 bytes (plain text, not HTML).
        """
        url = (
            f"{self._api_url}/{self._dataset}"
            f'?dokumento_id="{norm_id}"&select(tekstas_lt,priimtas)&limit(1)'
        )
        return self._fetch_with_retry(url)

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata JSON from data.gov.lt Spinta API.

        Args:
            norm_id: Document ID (dokumento_id), e.g. "TAR.47BB952431DA"

        Returns:
            JSON bytes with document metadata.
        """
        url = (
            f"{self._api_url}/{self._dataset}"
            f'?dokumento_id="{norm_id}"&select({_META_FIELDS})&limit(1)'
        )
        return self._fetch_with_retry(url)

    def get_page(self, page_size: int = 100, cursor: str | None = None) -> bytes:
        """Fetch a page of documents from the Spinta API.

        Args:
            page_size: Number of results per page.
            cursor: Cursor token for pagination (from _page.next).

        Returns:
            JSON bytes with _data array and _page.next cursor.
        """
        url = (
            f"{self._api_url}/{self._dataset}"
            f"?select({_DISCOVERY_FIELDS})&sort(dokumento_id)&limit({page_size})"
        )
        if cursor:
            url += f'&page("{cursor}")'
        return self._fetch_with_retry(url)

    def get_page_by_date(
        self, target_date: str, page_size: int = 100, cursor: str | None = None
    ) -> bytes:
        """Fetch documents adopted on a specific date (server-side filter).

        Args:
            target_date: ISO date string (YYYY-MM-DD).
            page_size: Number of results per page.
            cursor: Cursor token for pagination.

        Returns:
            JSON bytes with _data array and _page.next cursor.
        """
        url = (
            f"{self._api_url}/{self._dataset}"
            f'?priimtas="{target_date}"'
            f"&select({_DISCOVERY_FIELDS})&sort(dokumento_id)&limit({page_size})"
        )
        if cursor:
            url += f'&page("{cursor}")'
        return self._fetch_with_retry(url)

    def close(self) -> None:
        self._session.close()

    # ── Internal helpers ──

    def _fetch_with_retry(self, url: str) -> bytes:
        """Fetch URL with exponential backoff retry."""
        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                time.sleep(self._delay)
                r = self._session.get(url, timeout=self._timeout)
                if r.status_code in (429, 503):
                    wait = 2**attempt
                    logger.warning("Rate limited (%d), waiting %ds", r.status_code, wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.content
            except requests.RequestException as exc:
                last_exc = exc
                wait = 2**attempt
                logger.warning("Request failed (attempt %d): %s", attempt + 1, exc)
                time.sleep(wait)
        raise ConnectionError(f"Failed after {self._max_retries} retries: {last_exc}")
