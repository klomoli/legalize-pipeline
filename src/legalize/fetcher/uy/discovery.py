"""Discovery of Uruguayan norms via IMPO number range iteration.

IMPO has no catalog/search endpoint. Discovery works by iterating through
sequential law numbers and checking which ones exist. Law numbers run from
~1 (1826) to ~20500 (2026), but post-1935 laws (9500+) are most reliable.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.uy.client import IMPOClient

logger = logging.getLogger(__name__)

# Collections to iterate during full discovery
DEFAULT_COLLECTIONS = ("leyes", "decretos-ley", "constitucion")


class IMPODiscovery(NormDiscovery):
    """Discovers Uruguayan norms by iterating number ranges on IMPO."""

    @classmethod
    def create(cls, source: dict) -> IMPODiscovery:
        collections = source.get("collections", list(DEFAULT_COLLECTIONS))
        law_number_max = source.get("law_number_max", 20500)
        return cls(collections=collections, law_number_max=law_number_max)

    def __init__(
        self,
        collections: list[str] | None = None,
        law_number_max: int = 20500,
    ) -> None:
        self._collections = collections or list(DEFAULT_COLLECTIONS)
        self._law_number_max = law_number_max

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield norm IDs for all collections.

        For 'leyes': iterates numbers 1 to law_number_max, trying common
        year values. Since law numbers are globally unique in Uruguay,
        the year in the URL is just a routing hint — IMPO redirects to
        the correct year if the number exists.

        For 'constitucion': yields the single known entry.
        For 'decretos-ley': iterates the 1973-1985 range.
        """
        assert isinstance(client, IMPOClient)

        for collection in self._collections:
            if collection == "constitucion":
                yield from self._discover_constitucion(client)
            elif collection == "leyes":
                yield from self._discover_leyes(client, **kwargs)
            elif collection == "decretos-ley":
                yield from self._discover_decretos_ley(client, **kwargs)
            else:
                logger.warning("Unknown collection: %s", collection)

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Check recent law numbers above the last known max.

        Tries numbers from last_known+1 to last_known+100.
        """
        assert isinstance(client, IMPOClient)
        start = kwargs.get("last_known_number", self._law_number_max)
        year = target_date.year

        for num in range(start + 1, start + 100):
            norm_id = f"leyes/{num}-{year}"
            data = client.get_text(norm_id)
            if data:
                logger.info("Found new law: %s", norm_id)
                yield norm_id

    def _discover_constitucion(self, client: IMPOClient) -> Iterator[str]:
        """Yield the single Constitution entry."""
        norm_id = "constitucion/1967-1967"
        data = client.get_text(norm_id)
        if data:
            yield norm_id

    def _discover_leyes(self, client: IMPOClient, **kwargs) -> Iterator[str]:
        """Iterate law numbers from 1 to max, yielding those that exist."""
        limit = kwargs.get("limit", self._law_number_max)
        start = kwargs.get("start", 1)
        found = 0

        for num in range(start, min(limit + 1, self._law_number_max + 1)):
            # IMPO accepts the number with any year — it redirects if the law exists.
            # Use 0000 as a placeholder year.
            norm_id = f"leyes/{num}-0000"
            data = client.get_text(norm_id)
            if data:
                found += 1
                if found % 100 == 0:
                    logger.info("Laws discovered so far: %d (at number %d)", found, num)
                yield norm_id

    def _discover_decretos_ley(self, client: IMPOClient, **kwargs) -> Iterator[str]:
        """Iterate decreto-ley numbers (1973-1985 period)."""
        limit = kwargs.get("limit", 16000)
        start = kwargs.get("start", 14000)

        for num in range(start, limit + 1):
            for year in range(1973, 1986):
                norm_id = f"decretos-ley/{num}-{year}"
                data = client.get_text(norm_id)
                if data:
                    yield norm_id
                    break  # found the right year, move to next number
