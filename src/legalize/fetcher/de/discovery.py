"""Discovery of German federal legislation via gesetze-im-internet.de TOC."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import date
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.de.client import GIIClient

logger = logging.getLogger(__name__)


class GIIDiscovery(NormDiscovery):
    """Discovers all federal laws from the GII table of contents XML.

    The TOC at https://www.gesetze-im-internet.de/gii-toc.xml lists ~6900 laws.
    Each <item> has a <link> pointing to the ZIP download URL.
    The norm_id is the URL slug extracted from the link.
    """

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield all URL slugs from the GII TOC."""
        assert isinstance(client, GIIClient)
        toc = client.get_toc()
        root = ET.fromstring(toc)

        for item in root.findall(".//item"):
            link = item.find("link")
            if link is None or not link.text:
                continue
            slug = self._extract_slug(link.text)
            if slug:
                yield slug

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield slugs of laws modified on or after target_date.

        Uses HTTP HEAD requests to check Last-Modified headers on each ZIP.
        This is expensive (~6900 HEAD requests) so it should be used with
        a cached slug list and only checked periodically.

        If 'slugs' is passed in kwargs, only those slugs are checked.
        Otherwise, discovers all slugs first from the TOC.
        """
        assert isinstance(client, GIIClient)
        slugs = kwargs.get("slugs") or list(self.discover_all(client))

        for slug in slugs:
            try:
                headers = client.head_zip(slug)
                last_mod = headers.get("Last-Modified", "")
                if not last_mod:
                    continue
                mod_date = parsedate_to_datetime(last_mod).date()
                if mod_date >= target_date:
                    logger.info("Changed: %s (Last-Modified: %s)", slug, last_mod)
                    yield slug
            except Exception:
                logger.debug("Could not check %s, skipping", slug)

    @staticmethod
    def _extract_slug(url: str) -> str | None:
        """Extract the law slug from a GII ZIP URL.

        Example: 'http://www.gesetze-im-internet.de/gg/xml.zip' -> 'gg'
        """
        match = re.search(r"gesetze-im-internet\.de/([^/]+)/xml\.zip", url)
        return match.group(1) if match else None
