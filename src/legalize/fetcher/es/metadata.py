"""Parser for BOE norm metadata.

Converts the response from endpoint /api/legislacion-consolidada/id/{id}/metadatos
into a domain NormMetadata.

Actual API structure (XML):
    <response>
      <status><code>200</code></status>
      <data>
        <metadatos>
          <identificador>BOE-A-1978-31229</identificador>
          <departamento codigo="1220">Cortes Generales</departamento>
          <rango codigo="1070">Constitución</rango>
          <fecha_disposicion>19781227</fecha_disposicion>
          <titulo>Constitución Española.</titulo>
          <fecha_publicacion>19781229</fecha_publicacion>
          <fecha_vigencia>19781229</fecha_vigencia>
          <estatus_derogacion>N</estatus_derogacion>
          <estado_consolidacion codigo="3">Finalizado</estado_consolidacion>
          <url_eli>https://www.boe.es/eli/es/c/1978/12/27/(1)</url_eli>
          <url_html_consolidada>https://www.boe.es/buscar/act.php?id=BOE-A-1978-31229</url_html_consolidada>
        </metadatos>
      </data>
    </response>
"""

from __future__ import annotations

import logging
from datetime import date

from lxml import etree

from legalize.models import NormMetadata, NormStatus, Rank
from legalize.fetcher.es.titulos import get_short_title

logger = logging.getLogger(__name__)

# Mapping of BOE rank texts (case-insensitive) to our enum
_RANK_TEXT_MAP: dict[str, Rank] = {
    "constitución": Rank.CONSTITUCION,
    "constitucion": Rank.CONSTITUCION,
    "ley orgánica": Rank.LEY_ORGANICA,
    "ley organica": Rank.LEY_ORGANICA,
    "ley": Rank.LEY,
    "real decreto-ley": Rank.REAL_DECRETO_LEY,
    "real decreto legislativo": Rank.REAL_DECRETO_LEGISLATIVO,
    "real decreto": Rank.REAL_DECRETO,
    "orden": Rank.ORDEN,
    "resolución": Rank.RESOLUCION,
    "resolucion": Rank.RESOLUCION,
    "acuerdo internacional": Rank.ACUERDO_INTERNACIONAL,
    "circular": Rank.CIRCULAR,
    "instrucción": Rank.INSTRUCCION,
    "instruccion": Rank.INSTRUCCION,
    "decreto": Rank.DECRETO,
    "acuerdo": Rank.ACUERDO,
    "reglamento": Rank.REGLAMENTO,
    "decreto-ley": Rank.REAL_DECRETO_LEY,
}

# Mapping of BOE rank codes to our enum
_RANK_CODE_MAP: dict[str, Rank] = {
    "1070": Rank.CONSTITUCION,
    "1010": Rank.LEY_ORGANICA,
    "1020": Rank.LEY,
    "1040": Rank.REAL_DECRETO_LEY,
    "1050": Rank.REAL_DECRETO_LEGISLATIVO,
    "1290": Rank.LEY_ORGANICA,  # alternate code
    "1300": Rank.LEY,  # alternate code
    "1060": Rank.REAL_DECRETO,
    "1080": Rank.ORDEN,
    "1130": Rank.RESOLUCION,
    "1170": Rank.ACUERDO_INTERNACIONAL,
    "1190": Rank.CIRCULAR,
    "1200": Rank.INSTRUCCION,
    "1030": Rank.DECRETO,
    "1160": Rank.ACUERDO,
}


def _text_of(parent: etree._Element, tag: str) -> str:
    """Extracts the text of a sub-element, or '' if it does not exist."""
    el = parent.find(tag)
    if el is not None and el.text:
        return el.text.strip()
    return ""


def _code_of(parent: etree._Element, tag: str) -> str:
    """Extracts the 'codigo' attribute of a sub-element."""
    el = parent.find(tag)
    if el is not None:
        return el.get("codigo", "")
    return ""


def _parse_date_boe(text: str) -> date | None:
    """Parses BOE date: YYYYMMDD → date. Returns None for 99999999 (indefinite)."""
    if not text or len(text) < 8 or text.strip() == "99999999":
        return None
    try:
        parsed = date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        if parsed.year > 2100:
            return None
        return parsed
    except (ValueError, IndexError):
        logger.warning("Unparseable date: %s", text)
        return None


def _parse_rank(meta: etree._Element) -> Rank | None:
    """Resolves the rank from code or text."""
    code = _code_of(meta, "rango")
    if code and code in _RANK_CODE_MAP:
        return _RANK_CODE_MAP[code]

    text = _text_of(meta, "rango").lower()
    return _RANK_TEXT_MAP.get(text)


def _parse_status(meta: etree._Element) -> NormStatus:
    """Determines the validity status from BOE flags."""
    repeal_status = _text_of(meta, "estatus_derogacion")
    if repeal_status == "T":
        return NormStatus.REPEALED
    if repeal_status == "P":
        return NormStatus.PARTIALLY_REPEALED
    return NormStatus.IN_FORCE


def _infer_rank_from_title(title: str) -> Rank | None:
    """Attempts to infer the rank from the title."""
    lower = title.lower()
    if "constitución" in lower or "constitucion" in lower:
        return Rank.CONSTITUCION
    if "ley orgánica" in lower or "ley organica" in lower:
        return Rank.LEY_ORGANICA
    if "real decreto legislativo" in lower:
        return Rank.REAL_DECRETO_LEGISLATIVO
    if "real decreto-ley" in lower:
        return Rank.REAL_DECRETO_LEY
    if lower.startswith("ley "):
        return Rank.LEY
    if "real decreto" in lower and "ley" not in lower and "legislativo" not in lower:
        return Rank.REAL_DECRETO
    if lower.startswith("orden"):
        return Rank.ORDEN
    if lower.startswith("resolución") or lower.startswith("resolucion"):
        return Rank.RESOLUCION
    return None


# BOE departamento code → ELI jurisdiction code
# BOE departamento code → ELI jurisdiction code
# Some CCAA have multiple codes (name changes over time)
_DEPT_TO_JURISDICTION: dict[str, str] = {
    "8010": "es-an",  # Andalucía
    "8020": "es-ar",  # Aragón
    "8030": "es-cn",  # Canarias
    "8040": "es-cb",  # Cantabria
    "8060": "es-cm",  # Castilla-La Mancha
    "8070": "es-ct",  # Cataluña
    "8080": "es-ex",  # Extremadura
    "8090": "es-ga",  # Galicia
    "8100": "es-mc",  # Murcia
    "8110": "es-ri",  # La Rioja
    "8120": "es-ib",  # Illes Balears (código antiguo)
    "8121": "es-ib",  # Illes Balears (código actual)
    "8131": "es-md",  # Madrid
    "8140": "es-pv",  # País Vasco
    "8150": "es-as",  # Asturias
    "8161": "es-vc",  # Comunidad Valenciana
    "8162": "es-vc",  # Comunitat Valenciana (nombre en valenciano)
    "8170": "es-nc",  # Navarra
    "9531": "es-cl",  # Castilla y León
}


def _extract_jurisdiction(meta: etree._Element) -> str | None:
    """Extract autonomous community jurisdiction from BOE metadata.

    Uses the departamento code to determine the ELI jurisdiction.
    Returns None for state-level legislation (ambito=1).
    """
    scope_code = _code_of(meta, "ambito")
    if scope_code != "2":
        return None

    dept_code = _code_of(meta, "departamento")
    jurisdiction = _DEPT_TO_JURISDICTION.get(dept_code)

    if jurisdiction is None:
        # Fallback: try to extract from ELI URL (e.g., /eli/es-pv/l/...)
        eli = _text_of(meta, "url_eli")
        if eli and "/eli/" in eli:
            parts = eli.split("/eli/")[1].split("/")
            if parts and parts[0].startswith("es-"):
                jurisdiction = parts[0]

    return jurisdiction


def parse_metadata(xml_data: bytes, id_boe: str) -> NormMetadata:
    """Parses the XML response from the BOE /metadatos endpoint.

    Args:
        xml_data: Raw XML from the endpoint.
        id_boe: BOE identifier (fallback if not in XML).

    Returns:
        Parsed NormMetadata.

    Raises:
        ValueError: If minimum information cannot be extracted.
    """
    root = etree.fromstring(xml_data)

    # Navigate to <metadatos> inside <response><data>
    meta = root.find(".//metadatos")
    if meta is None:
        raise ValueError(f"<metadatos> not found in response for {id_boe}")

    identifier = _text_of(meta, "identificador") or id_boe
    title = _text_of(meta, "titulo") or id_boe
    short_title = get_short_title(identifier, title)
    department = _text_of(meta, "departamento")

    rank = _parse_rank(meta)
    if rank is None:
        rank = _infer_rank_from_title(title)
    if rank is None:
        logger.warning("Unrecognized rank for %s, using OTRO as fallback", id_boe)
        rank = Rank.OTRO

    pub_date = _parse_date_boe(_text_of(meta, "fecha_publicacion"))
    if pub_date is None:
        raise ValueError(f"Could not extract publication date for {id_boe}")

    effective_date = _parse_date_boe(_text_of(meta, "fecha_vigencia"))
    status = _parse_status(meta)

    source_url = (
        _text_of(meta, "url_eli")
        or _text_of(meta, "url_html_consolidada")
        or f"https://www.boe.es/buscar/act.php?id={identifier}"
    )

    # Detect autonomous community jurisdiction from ELI URL or ambito
    jurisdiction = _extract_jurisdiction(meta)

    return NormMetadata(
        title=title,
        short_title=short_title,
        identifier=identifier,
        country="es",
        rank=rank,
        publication_date=pub_date,
        status=status,
        department=department,
        source=source_url,
        jurisdiction=jurisdiction,
        last_modified=effective_date,
    )
