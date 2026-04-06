"""Discovery of Lithuanian legal acts via the data.gov.lt Spinta API.

Uses server-side filtering by rusis (act type) and offset-based pagination.
The Spinta API does not return pagination cursors, so we use offset().
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.lt.client import TARClient

logger = logging.getLogger(__name__)

# Core law types — real legislation with lasting effect.
# ~15K norms. Excludes administrative acts (Nutarimas 67K, Įsakymas 162K,
# Dekretas 12K, Potvarkis 20K, Rezoliucija 1K, Sprendimas 102K).
CORE_RUSIS: tuple[str, ...] = (
    "Konstitucija",  # 3
    "Konstitucinis įstatymas",  # 22
    "Įstatymas",  # ~14,920
    "Kodeksas",  # 12
)

# Daily updates may include administrative acts that amend laws
DAILY_RUSIS: frozenset[str] = frozenset(
    CORE_RUSIS
    + (
        "Nutarimas",
        "Įsakymas",
        "Dekretas",
    )
)

_DISCOVERY_FIELDS = "dokumento_id,rusis"


class TARDiscovery(NormDiscovery):
    """Discovers legislative acts in the Lithuanian TAR catalog via data.gov.lt.

    Uses server-side rusis filtering + offset-based pagination.
    The norm ID is the dokumento_id field (e.g. "TAR.47BB952431DA").
    """

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield dokumento_id values for core law types.

        Queries each rusis type separately with server-side filtering,
        using offset-based pagination (Spinta API has no cursor support).
        """
        assert isinstance(client, TARClient)
        seen: set[str] = set()

        for rusis in CORE_RUSIS:
            offset = 0
            page_size = 500

            while True:
                url = (
                    f"{client._base_url}/{client._dataset}"
                    f'?rusis="{rusis}"'
                    f"&select({_DISCOVERY_FIELDS})"
                    f"&sort(dokumento_id)"
                    f"&limit({page_size})&offset({offset})"
                )
                raw = client._get(url)
                data = json.loads(raw)
                items = data.get("_data", [])

                if not items:
                    break

                for item in items:
                    doc_id = item.get("dokumento_id", "")
                    if doc_id and doc_id not in seen:
                        seen.add(doc_id)
                        yield doc_id

                if len(items) < page_size:
                    break
                offset += page_size

            logger.info("Discovered %s %s norms", rusis, len(seen))

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield dokumento_id values for legislative acts adopted on target_date.

        Uses server-side filtering by priimtas (adoption date).
        Includes broader rusis set since administrative acts can amend laws.
        """
        assert isinstance(client, TARClient)
        seen: set[str] = set()
        date_str = target_date.isoformat()
        offset = 0
        page_size = 500

        while True:
            url = (
                f"{client._base_url}/{client._dataset}"
                f'?priimtas="{date_str}"'
                f"&select({_DISCOVERY_FIELDS})"
                f"&sort(dokumento_id)"
                f"&limit({page_size})&offset({offset})"
            )
            raw = client._get(url)
            data = json.loads(raw)
            items = data.get("_data", [])

            if not items:
                break

            for item in items:
                rusis = item.get("rusis", "")
                if rusis not in DAILY_RUSIS:
                    continue
                doc_id = item.get("dokumento_id", "")
                if doc_id and doc_id not in seen:
                    seen.add(doc_id)
                    yield doc_id

            if len(items) < page_size:
                break
            offset += page_size
