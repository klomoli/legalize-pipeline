"""Tests for the Dutch BWB parser (country=nl)."""

from __future__ import annotations

import gzip
from datetime import date
from pathlib import Path

import pytest

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.nl.parser import (
    BWBMetadataParser,
    BWBTextParser,
    _cals_table_to_markdown,
    _inline_text,
)
from legalize.models import NormStatus, Rank
from legalize.transformer.markdown import render_norm_at_date
from lxml import etree

FIXTURES = Path(__file__).parent / "fixtures" / "nl"


# ─── Helpers ────────────────────────────────────────────────────────────────


def _load(name: str) -> bytes:
    """Load a fixture. Some fixtures are gzipped to stay under the 2 MB
    pre-commit cap; the loader transparently decompresses ``.xml.gz`` files
    so test code can keep referring to the plain ``.xml`` name."""
    path = FIXTURES / name
    if not path.exists():
        gz_path = path.with_suffix(path.suffix + ".gz")
        if gz_path.exists():
            return gzip.decompress(gz_path.read_bytes())
    return path.read_bytes()


@pytest.fixture(scope="module")
def text_parser() -> BWBTextParser:
    return BWBTextParser()


@pytest.fixture(scope="module")
def meta_parser() -> BWBMetadataParser:
    return BWBMetadataParser()


# ─── Country registry dispatch ──────────────────────────────────────────────


class TestCountryDispatch:
    def test_text_parser_is_registered(self):
        parser = get_text_parser("nl")
        assert isinstance(parser, BWBTextParser)

    def test_metadata_parser_is_registered(self):
        parser = get_metadata_parser("nl")
        assert isinstance(parser, BWBMetadataParser)


# ─── Metadata extraction ────────────────────────────────────────────────────


class TestMetadataParser:
    def test_constitution_metadata(self, meta_parser: BWBMetadataParser):
        data = _load("sample-constitution.xml")
        meta = meta_parser.parse(data, "BWBR0001840")
        assert meta.identifier == "BWBR0001840"
        assert meta.country == "nl"
        assert meta.rank == Rank("grondwet")
        assert "Grondwet" in meta.title
        assert meta.short_title == "Grondwet"
        assert meta.status == NormStatus.IN_FORCE
        assert meta.source == "https://wetten.overheid.nl/BWBR0001840"
        assert meta.publication_date == date(2023, 2, 22)

    def test_civil_code_metadata(self, meta_parser: BWBMetadataParser):
        data = _load("sample-code.xml")
        meta = meta_parser.parse(data, "BWBR0005291")
        assert meta.identifier == "BWBR0005291"
        assert meta.rank == Rank("wet")
        assert meta.short_title == "Burgerlijk Wetboek Boek 3"
        assert meta.title.startswith("Burgerlijk Wetboek Boek 3")

    def test_income_tax_metadata(self, meta_parser: BWBMetadataParser):
        data = _load("sample-ordinary-law.xml")
        meta = meta_parser.parse(data, "BWBR0011353")
        assert meta.rank == Rank("wet")
        assert meta.short_title == "Wet inkomstenbelasting 2001"

    def test_amvb_metadata(self, meta_parser: BWBMetadataParser):
        data = _load("sample-with-tables.xml")
        meta = meta_parser.parse(data, "BWBR0011825")
        assert meta.rank == Rank("amvb")
        assert meta.short_title == "Vreemdelingenbesluit 2000"

    def test_ministerial_regulation_metadata(self, meta_parser: BWBMetadataParser):
        data = _load("sample-regulation.xml")
        meta = meta_parser.parse(data, "BWBR0014493")
        assert meta.rank == Rank("ministeriele_regeling")

    def test_every_fixture_captures_core_extras(self, meta_parser: BWBMetadataParser):
        """Regression guard: each fixture must expose the core metadata keys.

        ``stam_id``/``version_id``/``internal_id`` come from ``wetgeving``,
        ``toestand_uri`` comes from ``toestand``, and ``original_publication``
        comes from ``meta-data/brondata/oorspronkelijk/publicatie``.
        """
        required = {"soort", "stam_id", "version_id", "toestand_uri"}
        for name in (
            "sample-constitution.xml",
            "sample-code.xml",
            "sample-ordinary-law.xml",
            "sample-with-tables.xml",
            "sample-regulation.xml",
        ):
            data = _load(name)
            meta = meta_parser.parse(data, name)
            keys = {k for k, _ in meta.extra}
            assert required.issubset(keys), f"{name} missing: {required - keys}"

    def test_filesystem_safe_identifier(self, meta_parser: BWBMetadataParser):
        meta = meta_parser.parse(_load("sample-constitution.xml"), "BWBR0001840")
        assert ":" not in meta.identifier
        assert " " not in meta.identifier
        for ch in '/\\*?"<>|':
            assert ch not in meta.identifier


# ─── Text parsing ───────────────────────────────────────────────────────────


class TestTextParser:
    def test_parses_all_fixtures(self, text_parser: BWBTextParser):
        for name in (
            "sample-constitution.xml",
            "sample-code.xml",
            "sample-ordinary-law.xml",
            "sample-with-tables.xml",
            "sample-regulation.xml",
        ):
            blocks = text_parser.parse_text(_load(name))
            assert blocks, f"no blocks parsed for {name}"

    def test_article_count_matches_source(self, text_parser: BWBTextParser):
        """Every ``<artikel>`` in the source must become exactly one Block."""
        for name in (
            "sample-constitution.xml",
            "sample-code.xml",
            "sample-ordinary-law.xml",
            "sample-with-tables.xml",
            "sample-regulation.xml",
        ):
            data = _load(name)
            tree = etree.fromstring(data)
            src_articles = len(list(tree.iter("artikel")))
            blocks = text_parser.parse_text(data)
            article_blocks = [b for b in blocks if b.block_type == "article"]
            assert len(article_blocks) == src_articles, (
                f"{name}: source has {src_articles} articles, parser produced {len(article_blocks)}"
            )

    def test_no_duplicate_block_ids(self, text_parser: BWBTextParser):
        for name in (
            "sample-constitution.xml",
            "sample-code.xml",
            "sample-ordinary-law.xml",
            "sample-with-tables.xml",
            "sample-regulation.xml",
        ):
            blocks = text_parser.parse_text(_load(name))
            ids = [b.id for b in blocks]
            assert len(ids) == len(set(ids)), f"{name}: duplicate block IDs"

    def test_constitution_article_1_text(self, text_parser: BWBTextParser):
        blocks = text_parser.parse_text(_load("sample-constitution.xml"))
        art1 = next(b for b in blocks if b.id == "art-1")
        text = " ".join(p.text for p in art1.versions[0].paragraphs)
        assert "Discriminatie" in text
        assert "is niet toegestaan" in text

    def test_tables_are_rendered(self, text_parser: BWBTextParser):
        """Vreemdelingenbesluit has 4 CALS tables; all must become MD tables."""
        blocks = text_parser.parse_text(_load("sample-with-tables.xml"))
        table_paragraphs: list[str] = []
        for b in blocks:
            for v in b.versions:
                for p in v.paragraphs:
                    if p.css_class == "table":
                        table_paragraphs.append(p.text)
        assert len(table_paragraphs) == 4, f"expected 4 tables, got {len(table_paragraphs)}"
        # Every table must contain the Markdown separator row
        for t in table_paragraphs:
            assert "| ---" in t

    def test_tax_law_tables_preserved(self, text_parser: BWBTextParser):
        """Wet inkomstenbelasting has 10 tables."""
        blocks = text_parser.parse_text(_load("sample-ordinary-law.xml"))
        tables = [
            p.text
            for b in blocks
            for v in b.versions
            for p in v.paragraphs
            if p.css_class == "table"
        ]
        assert len(tables) == 10

    def test_signatories_emitted(self, text_parser: BWBTextParser):
        blocks = text_parser.parse_text(_load("sample-ordinary-law.xml"))
        sigs = [b for b in blocks if b.block_type == "signatories"]
        assert len(sigs) == 1
        text = " ".join(p.text for p in sigs[0].versions[0].paragraphs)
        assert "Lasten en bevelen" in text

    def test_reform_extraction(self, text_parser: BWBTextParser):
        """Each unique (bron, inwerking) pair becomes a Reform."""
        data = _load("sample-constitution.xml")
        reforms = text_parser.extract_reforms(data)
        assert len(reforms) > 1  # the Grondwet has many reforms
        # First reform is the oldest
        first = reforms[0]
        assert first.date.year >= 1840

    def test_rendered_markdown_is_clean_utf8(self, text_parser, meta_parser):
        """End-to-end: the full Markdown round-trips as clean UTF-8."""
        import re as _re

        for name in (
            "sample-constitution.xml",
            "sample-code.xml",
            "sample-regulation.xml",
        ):
            data = _load(name)
            meta = meta_parser.parse(data, name)
            blocks = text_parser.parse_text(data)
            md = render_norm_at_date(meta, blocks, date.today(), include_all=True)
            # UTF-8 round trip
            md.encode("utf-8")
            # No replacement chars, no C0 controls (except \n)
            assert "\ufffd" not in md
            assert not _re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", md)
            # Frontmatter + body present
            assert md.startswith("---\n")
            assert md.count("\n---\n") >= 1
            assert "\n# " in md  # H1


# ─── Inline text helpers ────────────────────────────────────────────────────


class TestInlineText:
    def test_plain_text(self):
        el = etree.fromstring("<al>hello world</al>")
        assert _inline_text(el) == "hello world"

    def test_bold_nadruk(self):
        el = etree.fromstring('<al>a <nadruk type="halfvet">b</nadruk> c</al>')
        assert _inline_text(el) == "a **b** c"

    def test_italic_nadruk(self):
        el = etree.fromstring('<al>x <nadruk type="cursief">y</nadruk> z</al>')
        assert _inline_text(el) == "x *y* z"

    def test_full_bold_nadruk(self):
        el = etree.fromstring('<al><nadruk type="vet">81</nadruk></al>')
        assert _inline_text(el) == "**81**"

    def test_intref_to_portal_link(self):
        el = etree.fromstring(
            '<al>see <intref doc="jci1.3:c:BWBR0001840&amp;artikel=1">art. 1</intref>.</al>'
        )
        out = _inline_text(el)
        assert "[art. 1](https://wetten.overheid.nl/jci1.3:c:BWBR0001840&artikel=1)" in out

    def test_extref_with_href(self):
        el = etree.fromstring('<al>see <extref href="https://example.eu/x">EU ref</extref>.</al>')
        assert "[EU ref](https://example.eu/x)" in _inline_text(el)

    def test_extref_with_jci_doc(self):
        el = etree.fromstring('<al>see <extref doc="jci1.3:c:BWBR0012092">Wet</extref>.</al>')
        out = _inline_text(el)
        assert "[Wet](https://wetten.overheid.nl/jci1.3:c:BWBR0012092)" in out


# ─── CALS tables ────────────────────────────────────────────────────────────


class TestMultiExpression:
    """The multi-expression envelope turns several historical toestanden into
    multi-``Version`` Blocks so git commits reflect every real reform."""

    def _envelope(self, expressions: list[tuple[str, str]]) -> bytes:
        """Build a synthetic ``<bwb-multi-expression>`` envelope from article texts."""
        parts = [b"<bwb-multi-expression bwb-id='BWBR9999999'>"]
        for effective, article_text in expressions:
            parts.append(
                f"""
<expression effective-date='{effective}'>
<toestand bwb-id='BWBR9999999' inwerkingtreding='{effective}'>
  <wetgeving dtdversie='2.0' soort='wet' xml:lang='nl'>
    <intitule>Testwet van {effective}</intitule>
    <citeertitel status='officieel'>Testwet</citeertitel>
    <wet-besluit>
      <wettekst>
        <artikel bron='Stb.{effective[:4]}-1' inwerking='{effective}'>
          <kop><label>Artikel</label><nr>1</nr></kop>
          <al>{article_text}</al>
        </artikel>
      </wettekst>
    </wet-besluit>
    <meta-data/>
  </wetgeving>
</toestand>
</expression>
                """.encode("utf-8")
            )
        parts.append(b"</bwb-multi-expression>")
        return b"".join(parts)

    def test_multi_version_blocks(self, text_parser: BWBTextParser):
        data = self._envelope(
            [
                ("2000-01-01", "original text"),
                ("2010-06-01", "amended text"),
                ("2020-03-15", "rewritten text"),
            ]
        )
        blocks = text_parser.parse_text(data)
        art_blocks = [b for b in blocks if b.block_type == "article"]
        assert len(art_blocks) == 1
        assert len(art_blocks[0].versions) == 3
        texts = [v.paragraphs[-1].text for v in art_blocks[0].versions]
        assert texts == ["original text", "amended text", "rewritten text"]

    def test_unchanged_text_is_deduped(self, text_parser: BWBTextParser):
        data = self._envelope(
            [
                ("2000-01-01", "same text"),
                ("2005-01-01", "same text"),
                ("2010-01-01", "different text"),
                ("2015-01-01", "different text"),
                ("2020-01-01", "final text"),
            ]
        )
        blocks = text_parser.parse_text(data)
        art = next(b for b in blocks if b.block_type == "article")
        # 3 distinct contents → 3 versions, not 5
        assert len(art.versions) == 3
        assert [v.paragraphs[-1].text for v in art.versions] == [
            "same text",
            "different text",
            "final text",
        ]

    def test_metadata_uses_latest_expression(self, meta_parser: BWBMetadataParser):
        data = self._envelope(
            [
                ("2000-01-01", "old"),
                ("2020-01-01", "new"),
            ]
        )
        meta = meta_parser.parse(data, "BWBR9999999")
        assert meta.publication_date == date(2020, 1, 1)
        assert meta.title.startswith("Testwet")

    def test_reforms_chronological(self, text_parser: BWBTextParser):
        data = self._envelope(
            [
                ("2000-01-01", "v1"),
                ("2010-01-01", "v2"),
                ("2020-01-01", "v3"),
            ]
        )
        reforms = text_parser.extract_reforms(data)
        assert [r.date for r in reforms] == [
            date(2000, 1, 1),
            date(2010, 1, 1),
            date(2020, 1, 1),
        ]


class TestCalsTable:
    def test_simple_3x2_table(self):
        xml = """
        <table>
          <tgroup cols="3">
            <colspec colname="col1"/>
            <colspec colname="col2"/>
            <colspec colname="col3"/>
            <thead>
              <row>
                <entry colname="col1"><al>A</al></entry>
                <entry colname="col2"><al>B</al></entry>
                <entry colname="col3"><al>C</al></entry>
              </row>
            </thead>
            <tbody>
              <row>
                <entry colname="col1"><al>1</al></entry>
                <entry colname="col2"><al>2</al></entry>
                <entry colname="col3"><al>3</al></entry>
              </row>
            </tbody>
          </tgroup>
        </table>
        """
        md = _cals_table_to_markdown(etree.fromstring(xml))
        assert "| A | B | C |" in md
        assert "| --- | --- | --- |" in md
        assert "| 1 | 2 | 3 |" in md

    def test_colspan_duplicates_cell(self):
        """CALS colspan is rendered as repeated text (MD has no native colspan)."""
        xml = """
        <table>
          <tgroup cols="4">
            <colspec colname="col1"/>
            <colspec colname="col2"/>
            <colspec colname="col3"/>
            <colspec colname="col4"/>
            <thead>
              <row>
                <entry colname="col1"><al>A</al></entry>
                <entry colname="col2"><al>B</al></entry>
                <entry namest="col3" nameend="col4"><al>CD</al></entry>
              </row>
            </thead>
            <tbody>
              <row>
                <entry colname="col1"><al>1</al></entry>
                <entry colname="col2"><al>2</al></entry>
                <entry colname="col3"><al>3</al></entry>
                <entry colname="col4"><al>4</al></entry>
              </row>
            </tbody>
          </tgroup>
        </table>
        """
        md = _cals_table_to_markdown(etree.fromstring(xml))
        assert "| A | B | CD | CD |" in md
        assert "| 1 | 2 | 3 | 4 |" in md

    def test_pipe_escape_in_cell(self):
        xml = """
        <table>
          <tgroup cols="1">
            <colspec colname="col1"/>
            <tbody>
              <row>
                <entry colname="col1"><al>a | b</al></entry>
              </row>
            </tbody>
          </tgroup>
        </table>
        """
        md = _cals_table_to_markdown(etree.fromstring(xml))
        assert "a \\| b" in md
