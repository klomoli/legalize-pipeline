"""Discovery of Austrian Bundesrecht norms via the RIS OGD API."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.at.client import RISClient


class RISDiscovery(NormDiscovery):
    """Discovers all Gesetze (grouped by Gesetzesnummer) in the RIS catalog."""

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield all unique Gesetzesnummern in BrKons (Bundesrecht konsolidiert).

        Paginates through the full catalog (~437k NOR entries) and yields
        unique Gesetzesnummern as stable IDs. Each Gesetzesnummer groups all
        paragraphs of one law together.
        """
        assert isinstance(client, RISClient)
        seen: set[str] = set()
        page = 1
        page_size = 100

        while True:
            raw = client.get_page(page=page, page_size=page_size)
            data = json.loads(raw)
            results = data["OgdSearchResult"]["OgdDocumentResults"]
            total = int(results["Hits"]["#text"])

            refs = results.get("OgdDocumentReference", [])
            if isinstance(refs, dict):
                refs = [refs]

            for ref in refs:
                br = ref["Data"]["Metadaten"]["Bundesrecht"]["BrKons"]
                gesnr = br.get("Gesetzesnummer", "")
                if gesnr and gesnr not in seen:
                    seen.add(gesnr)
                    yield gesnr

            fetched_so_far = (page - 1) * page_size + len(refs)
            if fetched_so_far >= total or not refs:
                break
            page += 1

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield Gesetzesnummern updated on target_date.

        The RIS API ignores the Geaendert query parameter, so we use
        ImRisSeit=EinerWoche to get recent changes and filter client-side
        by the Allgemein.Geaendert field matching the target date.
        """
        assert isinstance(client, RISClient)
        seen: set[str] = set()
        date_str = target_date.strftime("%Y-%m-%d")
        page = 1
        page_size = 100

        while True:
            raw = client.get_page(page=page, page_size=page_size, ImRisSeit="EinerWoche")
            data = json.loads(raw)
            results = data["OgdSearchResult"].get("OgdDocumentResults")
            if not results:
                break

            total = int(results["Hits"]["#text"])
            refs = results.get("OgdDocumentReference", [])
            if isinstance(refs, dict):
                refs = [refs]

            for ref in refs:
                geaendert = ref["Data"]["Metadaten"].get("Allgemein", {}).get("Geaendert", "")
                if geaendert != date_str:
                    continue
                br = ref["Data"]["Metadaten"]["Bundesrecht"]["BrKons"]
                gesnr = br.get("Gesetzesnummer", "")
                if gesnr and gesnr not in seen:
                    seen.add(gesnr)
                    yield gesnr

            fetched_so_far = (page - 1) * page_size + len(refs)
            if fetched_so_far >= total or not refs:
                break
            page += 1
