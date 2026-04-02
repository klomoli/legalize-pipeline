"""Tests for the Uruguayan IMPO parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from legalize.countries import (
    get_client_class,
    get_discovery_class,
    get_metadata_parser,
    get_text_parser,
    supported_countries,
)
from legalize.fetcher.uy.client import IMPOClient
from legalize.fetcher.uy.discovery import IMPODiscovery
from legalize.fetcher.uy.parser import (
    IMPOMetadataParser,
    IMPOTextParser,
    _decode_json,
    _make_identifier,
    _parse_date,
    _strip_html,
)
from legalize.models import NormMetadata, NormStatus
from legalize.transformer.slug import norm_to_filepath

FIXTURES = Path(__file__).parent / "fixtures"


# ─── Helpers ───


def _load_fixture(name: str) -> bytes:
    """Load a fixture as Latin-1 bytes (matching IMPO encoding)."""
    text = (FIXTURES / name).read_text(encoding="utf-8")
    return text.encode("latin-1")


# ─── Unit tests for parsing helpers ───


class TestParsingHelpers:
    def test_parse_date_dd_mm_yyyy(self):
        assert _parse_date("09/11/2021") == date(2021, 11, 9)

    def test_parse_date_iso_format(self):
        assert _parse_date("2021-11-09") == date(2021, 11, 9)

    def test_parse_date_empty(self):
        assert _parse_date("") is None
        assert _parse_date("  ") is None

    def test_parse_date_invalid(self):
        assert _parse_date("not-a-date") is None

    def test_strip_html_basic(self):
        assert _strip_html('<font color="#0000FF">Hello</font>') == "Hello"

    def test_strip_html_nested(self):
        assert _strip_html("<a href='x'><b>Text</b></a>") == "Text"

    def test_strip_html_empty(self):
        assert _strip_html("") == ""
        assert _strip_html(None) == ""

    def test_decode_json_latin1(self):
        raw = '{"key": "value"}'.encode("latin-1")
        assert _decode_json(raw) == {"key": "value"}

    def test_decode_json_utf8(self):
        raw = '{"key": "value"}'.encode("utf-8")
        assert _decode_json(raw) == {"key": "value"}

    def test_make_identifier_ley(self):
        doc = {"tipoNorma": "Ley", "nroNorma": "19996", "anioNorma": 2021}
        assert _make_identifier(doc, "leyes/19996-2021") == "UY-ley-19996"

    def test_make_identifier_decreto_ley(self):
        doc = {"tipoNorma": "Decreto Ley", "nroNorma": "14261", "anioNorma": 1974}
        assert _make_identifier(doc, "decretos-ley/14261-1974") == "UY-decreto-ley-14261"

    def test_make_identifier_constitucion(self):
        doc = {"tipoNorma": "CONSTITUCION DE LA REPUBLICA"}
        assert _make_identifier(doc, "constitucion/1967-1967") == "UY-constitucion-1967"


# ─── IMPOTextParser tests ───


class TestIMPOTextParser:
    def setup_method(self):
        self.parser = IMPOTextParser()

    def test_parse_ley_blocks(self):
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        # 4 articles + 2 section headings = 6 blocks
        assert len(blocks) >= 4

    def test_parse_ley_article_text(self):
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        # Find the first article block (not heading)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert len(article_blocks) >= 3  # articles 1, 2, 3 (placeholder), 4

        first = article_blocks[0]
        assert first.id == "art-1"
        assert "plataformas digitales" in first.versions[0].paragraphs[0].text

    def test_parse_ley_section_headings(self):
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        heading_blocks = [b for b in blocks if b.block_type == "heading"]
        assert len(heading_blocks) >= 1
        assert "SECCION I" in heading_blocks[0].title

    def test_parse_placeholder_article(self):
        """Articles with textoArticulo='(*)' should be parsed with note content."""
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        # Article 3 has text=(*), should have a nota paragraph
        art3 = [b for b in article_blocks if b.id == "art-3"]
        assert len(art3) == 1
        paras = art3[0].versions[0].paragraphs
        assert any(p.css_class == "nota" for p in paras)

    def test_parse_version_date(self):
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert article_blocks[0].versions[0].publication_date == date(2021, 11, 9)

    def test_parse_empty_data(self):
        assert self.parser.parse_text(b"") == []

    def test_parse_constitucion(self):
        data = _load_fixture("impo-constitucion-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert len(article_blocks) == 3
        first = article_blocks[0]
        assert "Republica Oriental del Uruguay" in first.versions[0].paragraphs[0].text

    def test_parse_constitucion_nested_headings(self):
        """Constitution has nested section/chapter headings."""
        data = _load_fixture("impo-constitucion-sample.json")
        blocks = self.parser.parse_text(data)
        heading_blocks = [b for b in blocks if b.block_type == "heading"]
        titles = [b.title for b in heading_blocks]
        assert any("SECCION I" in t for t in titles)
        assert any("CAPITULO I" in t for t in titles)

    def test_parse_decreto_ley(self):
        data = _load_fixture("impo-decreto-ley-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert len(article_blocks) == 2
        assert "tributos" in article_blocks[0].versions[0].paragraphs[0].text

    def test_extract_reforms_returns_list(self):
        data = _load_fixture("impo-ley-sample.json")
        reforms = self.parser.extract_reforms(data)
        assert isinstance(reforms, list)
        assert len(reforms) == 1
        assert reforms[0].date == date(2021, 11, 9)


# ─── IMPOMetadataParser tests ───


class TestIMPOMetadataParser:
    def setup_method(self):
        self.parser = IMPOMetadataParser()

    def test_parse_ley_metadata(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert isinstance(meta, NormMetadata)
        assert meta.country == "uy"
        assert meta.identifier == "UY-ley-19996"

    def test_ley_rank(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert str(meta.rank) == "ley"

    def test_ley_title(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert "19996" in meta.title
        assert "PLATAFORMAS DIGITALES" in meta.title

    def test_ley_date(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert meta.publication_date == date(2021, 11, 9)

    def test_ley_source(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert meta.source == "https://www.impo.com.uy/bases/leyes/19996-2021"

    def test_ley_status(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert meta.status == NormStatus.IN_FORCE

    def test_constitucion_metadata(self):
        data = _load_fixture("impo-constitucion-sample.json")
        meta = self.parser.parse(data, "constitucion/1967-1967")
        assert meta.identifier == "UY-constitucion-1967"
        assert str(meta.rank) == "constitucion"
        assert meta.publication_date == date(1967, 2, 2)

    def test_decreto_ley_metadata(self):
        data = _load_fixture("impo-decreto-ley-sample.json")
        meta = self.parser.parse(data, "decretos-ley/14261-1974")
        assert meta.identifier == "UY-decreto-ley-14261"
        assert str(meta.rank) == "decreto_ley"
        assert meta.publication_date == date(1974, 9, 9)

    def test_empty_data_raises(self):
        import pytest

        with pytest.raises(ValueError, match="Empty data"):
            self.parser.parse(b"", "leyes/1-0000")


# ─── Country dispatch tests ───


class TestCountriesDispatch:
    def test_uy_in_supported_countries(self):
        assert "uy" in supported_countries()

    def test_get_client_class_uy(self):
        cls = get_client_class("uy")
        assert cls is IMPOClient

    def test_get_discovery_class_uy(self):
        cls = get_discovery_class("uy")
        assert cls is IMPODiscovery

    def test_get_text_parser_uy(self):
        parser = get_text_parser("uy")
        assert isinstance(parser, IMPOTextParser)

    def test_get_metadata_parser_uy(self):
        parser = get_metadata_parser("uy")
        assert isinstance(parser, IMPOMetadataParser)


# ─── Slug tests ───


class TestSlugUruguay:
    def test_ley_path(self):
        meta = NormMetadata(
            title="Test",
            short_title="Test",
            identifier="UY-ley-19996",
            country="uy",
            rank="ley",
            publication_date=date(2021, 11, 9),
            status=NormStatus.IN_FORCE,
            department="",
            source="https://www.impo.com.uy/bases/leyes/19996-2021",
        )
        assert norm_to_filepath(meta) == "uy/UY-ley-19996.md"

    def test_constitucion_path(self):
        meta = NormMetadata(
            title="Constitucion",
            short_title="Constitucion",
            identifier="UY-constitucion-1967",
            country="uy",
            rank="constitucion",
            publication_date=date(1967, 2, 2),
            status=NormStatus.IN_FORCE,
            department="",
            source="https://www.impo.com.uy/bases/constitucion/1967-1967",
        )
        assert norm_to_filepath(meta) == "uy/UY-constitucion-1967.md"

    def test_decreto_ley_path(self):
        meta = NormMetadata(
            title="Test",
            short_title="Test",
            identifier="UY-decreto-ley-14261",
            country="uy",
            rank="decreto_ley",
            publication_date=date(1974, 9, 9),
            status=NormStatus.IN_FORCE,
            department="",
            source="https://www.impo.com.uy/bases/decretos-ley/14261-1974",
        )
        assert norm_to_filepath(meta) == "uy/UY-decreto-ley-14261.md"
