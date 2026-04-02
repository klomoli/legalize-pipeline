"""YAML frontmatter generation for norm Markdown files.

Generic multi-country format:
  ---
  titulo: "Constitución Española"
  identificador: "BOE-A-1978-31229"
  pais: "es"
  rango: "constitucion"
  fecha_publicacion: "1978-12-29"
  ultima_actualizacion: "2024-02-17"
  estado: "vigente"
  fuente: "https://www.boe.es/eli/es/c/1978/12/27/(1)"
  ---
"""

from __future__ import annotations

from datetime import date

from legalize.models import NormMetadata, NormStatus


def render_frontmatter(metadata: NormMetadata, version_date: date) -> str:
    """Generates the YAML frontmatter block for a norm at a given date."""
    clean_title = _clean_title(metadata.title)

    lines = [
        "---",
        f'titulo: "{_escape_yaml(clean_title)}"',
        f'identificador: "{metadata.identifier}"',
        f'pais: "{metadata.country}"',
    ]

    if metadata.jurisdiction:
        lines.append(f'jurisdiccion: "{metadata.jurisdiction}"')

    lines.extend(
        [
            f'rango: "{metadata.rank}"',
            f'fecha_publicacion: "{metadata.publication_date.isoformat()}"',
            f'ultima_actualizacion: "{version_date.isoformat()}"',
            f'estado: "{metadata.status.value if isinstance(metadata.status, NormStatus) else metadata.status}"',
            f'fuente: "{metadata.source}"',
        ]
    )

    if metadata.pdf_url:
        lines.append(f'url_pdf: "{metadata.pdf_url}"')

    lines.append("---")
    lines.append("")

    return "\n".join(lines)


def _escape_yaml(text: str) -> str:
    """Escapes double quotes in YAML values."""
    return text.replace('"', '\\"')


def _clean_title(raw_title: str) -> str:
    """Cleans the title: remove trailing period, normalize spaces."""
    return raw_title.rstrip(". ").strip()
