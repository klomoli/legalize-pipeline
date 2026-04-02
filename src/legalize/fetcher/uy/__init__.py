"""Uruguay (UY) — IMPO legislative fetcher components."""

from legalize.fetcher.uy.client import IMPOClient
from legalize.fetcher.uy.discovery import IMPODiscovery
from legalize.fetcher.uy.parser import IMPOMetadataParser, IMPOTextParser

__all__ = [
    "IMPOClient",
    "IMPODiscovery",
    "IMPOTextParser",
    "IMPOMetadataParser",
]
