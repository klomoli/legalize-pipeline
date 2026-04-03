"""Parser for Lithuanian data.gov.lt metadata (JSON) and text (plain text).

Both metadata and full text come from the data.gov.lt Spinta API.
The tekstas_lt field contains the full law text as plain text with
newline-separated paragraphs.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import (
    Block,
    NormMetadata,
    NormStatus,
    Paragraph,
    Rank,
    Version,
)

# Map Lithuanian act type names (rusis) to rank strings
RUSIS_TO_RANK: dict[str, str] = {
    "Konstitucija": "konstitucija",
    "Konstitucinis įstatymas": "konstitucinis_istatymas",
    "Įstatymas": "istatymas",
    "Kodeksas": "istatymas",
    "Nutarimas": "nutarimas",
    "Įsakymas": "isakymas",
    "Sprendimas": "sprendimas",
    "Potvarkis": "potvarkis",
    "Dekretas": "dekretas",
    "Rezoliucija": "rezoliucija",
}

# Map Lithuanian status values (galioj_busena) to NormStatus
# Only 3 values exist in the DB: galioja (380K), negalioja (89K), neįsigaliojęs (413)
STATUS_MAP: dict[str, NormStatus] = {
    "galioja": NormStatus.IN_FORCE,
    "negalioja": NormStatus.REPEALED,
    "neįsigaliojęs": NormStatus.IN_FORCE,  # not yet in force — treat as in_force
}

# Lithuanian structural element patterns
_ARTICLE_RE = re.compile(r"(?P<num>\d+)\s*straipsnis\b", re.IGNORECASE)
_CHAPTER_RE = re.compile(r"(?P<num>[IVXLCDM]+)\s*(?:skyrius|SKYRIUS)\b", re.IGNORECASE)
_SECTION_RE = re.compile(r"(?P<num>[IVXLCDM]+|\d+)\s*(?:skirsnis|SKIRSNIS)\b", re.IGNORECASE)
_PART_RE = re.compile(r"(?P<num>[IVXLCDM]+|\d+)\s*(?:dalis|DALIS)\b", re.IGNORECASE)


def _parse_date(s: str | None) -> date | None:
    """Parse ISO date string (YYYY-MM-DD)."""
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, IndexError):
        return None


def _classify_line(text: str) -> str:
    """Classify a text line by Lithuanian structural patterns."""
    if _ARTICLE_RE.search(text):
        return "article_heading"
    if _CHAPTER_RE.search(text):
        return "chapter_heading"
    if _SECTION_RE.search(text):
        return "section_heading"
    if _PART_RE.search(text):
        return "part_heading"
    return "text"


def _text_to_paragraphs(text: str) -> list[Paragraph]:
    """Convert plain text into a list of Paragraph objects.

    The tekstas_lt field from data.gov.lt is plain text with
    newline-separated lines (not HTML).
    """
    paragraphs: list[Paragraph] = []

    for line in text.split("\n"):
        line = line.strip()
        if not line or line == "\xa0":
            continue
        css_class = _classify_line(line)
        paragraphs.append(Paragraph(css_class=css_class, text=line))

    return paragraphs


class TARTextParser(TextParser):
    """Parses plain text from data.gov.lt tekstas_lt into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse data.gov.lt JSON response containing tekstas_lt.

        The input is JSON with _data[0].tekstas_lt containing the full text
        and optionally priimtas (adoption date) for block versioning.
        Groups consecutive paragraphs under article headings into blocks.
        """
        raw = data.decode("utf-8", errors="replace")
        pub_date = date(1900, 1, 1)

        # Handle JSON response from API
        try:
            api_data = json.loads(raw)
            items = api_data.get("_data", [])
            if not items:
                return []
            text = items[0].get("tekstas_lt", "")
            pub_date = _parse_date(items[0].get("priimtas")) or date(1900, 1, 1)
        except (json.JSONDecodeError, KeyError, IndexError):
            # Fall back to treating input as plain text
            text = raw

        if not text or not text.strip():
            return []

        paragraphs = _text_to_paragraphs(text)
        if not paragraphs:
            return []

        blocks: list[Block] = []
        current_id = "full"
        current_title = "Full text"
        current_paragraphs: list[Paragraph] = []
        block_index = 0

        for para in paragraphs:
            if para.css_class == "article_heading":
                if current_paragraphs:
                    blocks.append(
                        self._make_block(current_id, current_title, current_paragraphs, pub_date)
                    )
                match = _ARTICLE_RE.search(para.text)
                num = match.group("num") if match else str(block_index)
                current_id = f"str{num}"
                current_title = para.text
                current_paragraphs = [para]
                block_index += 1
            else:
                current_paragraphs.append(para)

        if current_paragraphs:
            blocks.append(self._make_block(current_id, current_title, current_paragraphs, pub_date))

        return blocks

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform timeline.

        data.gov.lt provides consolidated text; reform history requires
        cross-referencing amendment metadata. Returns empty list for now.
        """
        return []

    @staticmethod
    def _make_block(
        block_id: str,
        title: str,
        paragraphs: list[Paragraph],
        pub_date: date = date(1900, 1, 1),
    ) -> Block:
        """Create a Block with a single version from paragraphs."""
        version = Version(
            norm_id=block_id,
            publication_date=pub_date,
            effective_date=pub_date,
            paragraphs=tuple(paragraphs),
        )
        return Block(
            id=block_id,
            block_type="article" if block_id != "full" else "full",
            title=title,
            versions=(version,),
        )


class TARMetadataParser(MetadataParser):
    """Parses data.gov.lt Spinta API JSON into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse JSON metadata from data.gov.lt.

        Real API fields:
            dokumento_id → identifier
            pavadinimas → title
            alt_pavadinimas → short_title (fallback to pavadinimas)
            rusis → rank (via RUSIS_TO_RANK mapping)
            galioj_busena → status (via STATUS_MAP)
            priimtas → publication_date
            pakeista → last_modified (latest amendment date)
            isigalioja → last_modified fallback (entry into force)
            negalioja → used for status inference
            priemusi_inst → department
            nuoroda → source URL
        """
        api_data = json.loads(data)
        items = api_data.get("_data", [])

        if not items:
            raise ValueError(f"No metadata found for dokumento_id {norm_id}")

        item = items[0]

        title = item.get("pavadinimas", "").strip()
        short_title = (item.get("alt_pavadinimas") or "").strip() or title
        doc_id = item.get("dokumento_id", norm_id).strip()

        # Rank mapping
        rusis = item.get("rusis", "").strip()
        rank_str = RUSIS_TO_RANK.get(rusis, "kita")

        # Dates
        pub_date = _parse_date(item.get("priimtas")) or date(1900, 1, 1)
        expiry_date = _parse_date(item.get("negalioja"))

        # last_modified: use the latest date from pakeista (amendment dates),
        # falling back to isigalioja (entry into force date)
        pakeista = item.get("pakeista") or ""
        last_mod = None
        if pakeista:
            amendment_dates = [_parse_date(d.strip()) for d in pakeista.split(",")]
            valid_dates = [d for d in amendment_dates if d]
            if valid_dates:
                last_mod = max(valid_dates)
        if not last_mod:
            last_mod = _parse_date(item.get("isigalioja"))

        # Status from galioj_busena
        status_raw = item.get("galioj_busena", "").strip()
        status = STATUS_MAP.get(status_raw, NormStatus.IN_FORCE)
        if expiry_date and not status_raw:
            status = NormStatus.REPEALED

        institution = item.get("priemusi_inst", "").strip()
        source_url = item.get("nuoroda", "").strip()
        if not source_url:
            source_url = f"https://e-tar.lt/portal/lt/legalAct/{doc_id}"

        return NormMetadata(
            title=title,
            short_title=short_title,
            identifier=doc_id,
            country="lt",
            rank=Rank(rank_str),
            publication_date=pub_date,
            status=status,
            department=institution,
            source=source_url,
            last_modified=last_mod,
        )
