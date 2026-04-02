"""Tests for the BOE metadata parser."""

from datetime import date

from legalize.models import NormStatus, Rank
from legalize.fetcher.es.metadata import parse_metadata

# Real XML from the Constitution (captured from the API)
CONSTITUCION_META_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<response>
  <status><code>200</code><text>ok</text></status>
  <data>
    <metadatos>
      <fecha_actualizacion>20260224T130836Z</fecha_actualizacion>
      <identificador>BOE-A-1978-31229</identificador>
      <ambito codigo="1">Estatal</ambito>
      <departamento codigo="1220">Cortes Generales</departamento>
      <rango codigo="1070">Constitucion</rango>
      <fecha_disposicion>19781227</fecha_disposicion>
      <titulo>Constitucion Espanola.</titulo>
      <diario>Boletin Oficial del Estado</diario>
      <fecha_publicacion>19781229</fecha_publicacion>
      <diario_numero>311</diario_numero>
      <fecha_vigencia>19781229</fecha_vigencia>
      <estatus_derogacion>N</estatus_derogacion>
      <estatus_anulacion>N</estatus_anulacion>
      <vigencia_agotada>N</vigencia_agotada>
      <estado_consolidacion codigo="3">Finalizado</estado_consolidacion>
      <url_eli>https://www.boe.es/eli/es/c/1978/12/27/(1)</url_eli>
      <url_html_consolidada>https://www.boe.es/buscar/act.php?id=BOE-A-1978-31229</url_html_consolidada>
    </metadatos>
  </data>
</response>"""


class TestParseMetadatos:
    def test_parse_constitucion(self):
        meta = parse_metadata(CONSTITUCION_META_XML, "BOE-A-1978-31229")
        assert meta.identifier == "BOE-A-1978-31229"
        assert meta.rank == Rank.CONSTITUCION
        assert meta.publication_date == date(1978, 12, 29)
        assert meta.department == "Cortes Generales"
        assert meta.status == NormStatus.IN_FORCE

    def test_title(self):
        meta = parse_metadata(CONSTITUCION_META_XML, "BOE-A-1978-31229")
        assert "Constitucion" in meta.title

    def test_source_url(self):
        meta = parse_metadata(CONSTITUCION_META_XML, "BOE-A-1978-31229")
        assert meta.source.startswith("https://")

    def test_rank_from_code(self):
        """The rank is resolved from code '1070' = Constitution."""
        meta = parse_metadata(CONSTITUCION_META_XML, "BOE-A-1978-31229")
        assert meta.rank == Rank.CONSTITUCION

    def test_repealed_status(self):
        """estatus_derogacion='T' results in REPEALED."""
        xml = CONSTITUCION_META_XML.replace(
            b"<estatus_derogacion>N</estatus_derogacion>",
            b"<estatus_derogacion>T</estatus_derogacion>",
        )
        meta = parse_metadata(xml, "BOE-A-1978-31229")
        assert meta.status == NormStatus.REPEALED
