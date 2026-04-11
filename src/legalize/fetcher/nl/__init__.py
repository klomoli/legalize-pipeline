"""Netherlands (NL) — Basis Wetten Bestand (BWB) legislative fetcher components."""

from legalize.fetcher.nl.client import BWBClient
from legalize.fetcher.nl.discovery import BWBDiscovery
from legalize.fetcher.nl.parser import BWBMetadataParser, BWBTextParser

__all__ = [
    "BWBClient",
    "BWBDiscovery",
    "BWBTextParser",
    "BWBMetadataParser",
]
