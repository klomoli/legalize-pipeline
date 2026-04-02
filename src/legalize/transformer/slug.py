"""File path generation for norms.

Structure: {country_code}/{identifier}.md
The rank goes in the YAML frontmatter, not in the directory structure.

Example: es/BOE-A-1978-31229.md
         se/SFS-1962-700.md
"""

from __future__ import annotations

from legalize.models import NormMetadata


def norm_to_filepath(metadata: NormMetadata) -> str:
    """Generates the path for a norm file.

    State-level: '{country_code}/{identifier}.md'
      Example: 'es/BOE-A-2015-11430.md'

    Autonomous community: '{jurisdiction}/{identifier}.md'
      Example: 'es-pv/BOE-A-2020-615.md'
    """
    filename = f"{metadata.identifier}.md"
    if metadata.jurisdiction:
        return f"{metadata.jurisdiction}/{filename}"
    return f"{metadata.country}/{filename}"
