"""Discovery of Lithuanian legal acts via the data.gov.lt Spinta API."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.lt.client import TARClient

# Legislative act types to include (skip judicial: Nutartis)
LEGISLATIVE_RUSIS: frozenset[str] = frozenset(
    {
        "Konstitucija",
        "Konstitucinis įstatymas",
        "Įstatymas",
        "Kodeksas",
        "Nutarimas",
        "Įsakymas",
        "Dekretas",
        "Potvarkis",
        "Rezoliucija",
        "Sprendimas",  # Municipal/administrative decisions (savivaldybės)
    }
)


class TARDiscovery(NormDiscovery):
    """Discovers legislative acts in the Lithuanian TAR catalog via data.gov.lt.

    Uses cursor-based pagination with page("cursor") syntax.
    The norm ID is the dokumento_id field (e.g. "TAR.47BB952431DA").
    Filters out judicial decisions (Nutartis) which contain personal data.
    """

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield dokumento_id values for legislative acts only."""
        assert isinstance(client, TARClient)
        seen: set[str] = set()
        cursor: str | None = None

        while True:
            raw = client.get_page(page_size=100, cursor=cursor)
            data = json.loads(raw)
            items = data.get("_data", [])

            if not items:
                break

            for item in items:
                rusis = item.get("rusis", "")
                if rusis not in LEGISLATIVE_RUSIS:
                    continue
                doc_id = item.get("dokumento_id", "")
                if doc_id and doc_id not in seen:
                    seen.add(doc_id)
                    yield doc_id

            next_cursor = data.get("_page", {}).get("next")
            if not next_cursor:
                break
            cursor = next_cursor

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield dokumento_id values for legislative acts adopted on target_date.

        Uses server-side filtering by priimtas (adoption date) to avoid
        paginating the entire 469K catalog. Filters client-side by rusis.
        """
        assert isinstance(client, TARClient)
        seen: set[str] = set()
        date_str = target_date.isoformat()
        cursor: str | None = None

        while True:
            raw = client.get_page_by_date(date_str, page_size=100, cursor=cursor)
            data = json.loads(raw)
            items = data.get("_data", [])

            if not items:
                break

            for item in items:
                rusis = item.get("rusis", "")
                if rusis not in LEGISLATIVE_RUSIS:
                    continue
                doc_id = item.get("dokumento_id", "")
                if doc_id and doc_id not in seen:
                    seen.add(doc_id)
                    yield doc_id

            next_cursor = data.get("_page", {}).get("next")
            if not next_cursor:
                break
            cursor = next_cursor
