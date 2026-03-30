"""Tests for file path generation."""

from datetime import date

from legalize.models import EstadoNorma, NormaMetadata, Rango
from legalize.transformer.slug import norm_to_filepath


def _make_metadata(
    identificador: str = "BOE-A-2024-1",
    pais: str = "es",
    rango: Rango = Rango.LEY,
    jurisdiccion: str | None = None,
) -> NormaMetadata:
    return NormaMetadata(
        titulo="Test",
        titulo_corto="Test",
        identificador=identificador,
        pais=pais,
        rango=rango,
        fecha_publicacion=date(2024, 1, 1),
        estado=EstadoNorma.VIGENTE,
        departamento="Test",
        fuente="https://example.com",
        jurisdiccion=jurisdiccion,
    )


class TestNormaToFilepath:
    def test_state_level_uses_pais(self):
        meta = _make_metadata("BOE-A-2015-11430")
        assert norm_to_filepath(meta) == "es/BOE-A-2015-11430.md"

    def test_ccaa_uses_jurisdiccion(self):
        meta = _make_metadata("BOE-A-2020-615", jurisdiccion="es-pv")
        assert norm_to_filepath(meta) == "es-pv/BOE-A-2020-615.md"

    def test_france(self):
        meta = _make_metadata("JORF-001", pais="fr")
        assert norm_to_filepath(meta) == "fr/JORF-001.md"

    def test_filename_is_identificador(self):
        meta = _make_metadata("BOE-A-1978-31229")
        assert norm_to_filepath(meta).endswith("BOE-A-1978-31229.md")

    def test_no_rango_subfolder(self):
        """Rango does not affect the path — it's in the YAML frontmatter."""
        meta1 = _make_metadata("BOE-A-1978-31229", rango=Rango.CONSTITUCION)
        meta2 = _make_metadata("BOE-A-1978-31229", rango=Rango.LEY)
        assert norm_to_filepath(meta1) == norm_to_filepath(meta2)

    def test_flat_structure_no_subdirectories(self):
        """All paths must be exactly one level deep: {dir}/{file}.md"""
        countries = [
            ("BOE-A-2024-1", "es", None),
            ("JORF-001", "fr", None),
            ("SFS-1962-700", "se", None),
            ("AT-10002333", "at", None),
            ("BOE-A-2020-615", "es", "es-pv"),
        ]
        for identifier, country, jurisdiction in countries:
            meta = _make_metadata(identifier, pais=country, jurisdiccion=jurisdiction)
            path = norm_to_filepath(meta)
            parts = path.split("/")
            assert len(parts) == 2, (
                f"Path must be flat (dir/file.md), got {len(parts)} levels: {path}"
            )
            assert parts[1].endswith(".md"), f"File must end with .md: {path}"
            assert ".." not in path, f"Path must not contain '..': {path}"
