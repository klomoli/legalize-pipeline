"""Discovery of Dutch legal acts via the SRU search interface.

The KOOP SRU endpoint exposes the BWB collection at:
    https://zoekservice.overheid.nl/sru/Search?x-connection=BWB

For the full-catalog sweep we query with a "valid today" filter so that
each law appears at most once (one active toestand per regeling). For daily
updates we filter by ``dcterms.modified>=target_date``.

Total active laws (measured 2026-04-11): ~22,024 across all types.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date
from xml.etree import ElementTree as ET

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.nl.client import BWBClient

logger = logging.getLogger(__name__)

# SRU response XML namespaces
_NS = {
    "sru": "http://docs.oasis-open.org/ns/search-ws/sruResponse",
    "gzd": "http://standaarden.overheid.nl/sru",
    "dcterms": "http://purl.org/dc/terms/",
    "overheid": "http://standaarden.overheid.nl/owms/terms/",
    "overheidbwb": "http://standaarden.overheid.nl/bwb/terms/",
}

# SRU default page size. 500 is well-tested; 1000 is documented as the max.
_PAGE_SIZE = 500


def _parse_sru_response(data: bytes) -> tuple[int, list[dict[str, object]]]:
    """Parse an SRU searchRetrieveResponse into (total, records).

    Each record dict contains:
        identifier, title, type, authority, modified (date or None),
        toestand_uri, latest_xml_url, rechtsgebied (list),
        overheidsdomein (list), validity_start, validity_end
    """
    root = ET.fromstring(data)
    total_el = root.find("sru:numberOfRecords", _NS)
    total = int(total_el.text) if total_el is not None and total_el.text else 0

    records: list[dict[str, object]] = []
    for record in root.findall("sru:records/sru:record", _NS):
        gzd = record.find("sru:recordData/gzd:gzd", _NS)
        if gzd is None:
            continue
        meta = gzd.find("gzd:originalData/overheidbwb:meta", _NS)
        enriched = gzd.find("gzd:enrichedData", _NS)
        if meta is None:
            continue

        owmskern = meta.find("gzd:owmskern", _NS)
        bwbipm = meta.find("gzd:bwbipm", _NS)

        def _text(el: ET.Element | None, xpath: str) -> str:
            if el is None:
                return ""
            found = el.find(xpath, _NS)
            return (found.text or "").strip() if found is not None else ""

        def _texts(el: ET.Element | None, xpath: str) -> list[str]:
            if el is None:
                return []
            return [(e.text or "").strip() for e in el.findall(xpath, _NS) if e.text]

        identifier = _text(owmskern, "dcterms:identifier")
        if not identifier:
            continue

        latest_xml_url = ""
        if enriched is not None:
            loc = enriched.find("overheidbwb:locatie_toestand", _NS)
            if loc is not None and loc.text:
                latest_xml_url = loc.text.strip()

        rec = {
            "identifier": identifier,
            "title": _text(owmskern, "dcterms:title"),
            "type": _text(owmskern, "dcterms:type"),
            "authority": _text(owmskern, "overheid:authority"),
            "modified": _text(owmskern, "dcterms:modified"),
            "toestand_uri": _text(bwbipm, "overheidbwb:toestand"),
            "latest_xml_url": latest_xml_url,
            "rechtsgebied": _texts(bwbipm, "overheidbwb:rechtsgebied"),
            "overheidsdomein": _texts(bwbipm, "overheidbwb:overheidsdomein"),
            "onderwerp_verdrag": _texts(bwbipm, "overheidbwb:onderwerpVerdrag"),
            "validity_start": _text(bwbipm, "overheidbwb:geldigheidsperiode_startdatum"),
            "validity_end": _text(bwbipm, "overheidbwb:geldigheidsperiode_einddatum"),
        }
        records.append(rec)
    return total, records


class BWBDiscovery(NormDiscovery):
    """Discovery of BWB norm IDs via the SRU search interface.

    ``discover_all`` paginates through all currently-active regelingen
    (one toestand per regeling). ``discover_daily`` returns IDs of laws
    whose ``dcterms:modified`` is on or after the target date.
    """

    def discover_all(
        self,
        client: LegislativeClient,
        **kwargs,
    ) -> Iterator[str]:
        """Yield every BWB identifier whose current expression is valid today.

        Builds the query with ``geldigheidsdatum`` and ``zichtdatum`` set to
        today, which forces the SRU layer to return one record per law
        instead of one per historical toestand.
        """
        assert isinstance(client, BWBClient)

        today = date.today().isoformat()
        # Pinning both validity and view dates yields exactly one record per law.
        query = f"overheidbwb.geldigheidsdatum={today} and overheidbwb.zichtdatum={today}"

        seen: set[str] = set()
        start = 1
        while True:
            data = client.sru_search(query, start_record=start, maximum_records=_PAGE_SIZE)
            total, records = _parse_sru_response(data)
            if not records:
                break
            for rec in records:
                identifier = str(rec["identifier"])
                if identifier in seen:
                    continue
                seen.add(identifier)
                yield identifier
            start += _PAGE_SIZE
            if start > total:
                break
        logger.info("BWB discovery: yielded %d unique identifiers", len(seen))

    def discover_daily(
        self,
        client: LegislativeClient,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        """Yield BWB IDs whose ``dcterms:modified`` is >= target_date.

        SRU supports range operators on ``dcterms.modified`` — we use
        ``>=target_date`` and let the daily runner dedupe.
        """
        assert isinstance(client, BWBClient)

        iso = target_date.isoformat()
        query = f"dcterms.modified>={iso}"

        seen: set[str] = set()
        start = 1
        while True:
            data = client.sru_search(query, start_record=start, maximum_records=_PAGE_SIZE)
            total, records = _parse_sru_response(data)
            if not records:
                break
            for rec in records:
                identifier = str(rec["identifier"])
                if identifier in seen:
                    continue
                seen.add(identifier)
                yield identifier
            start += _PAGE_SIZE
            if start > total:
                break
