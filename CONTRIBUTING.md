# Contributing to Legalize

Thanks for your interest in Legalize! We especially welcome contributions that add new countries to the platform.

## The #1 contribution: add your country

The highest-impact contribution is adding a new country's legislation to the pipeline. This means writing a parser for your country's official gazette API or XML dump.

**Full guide:** [docs/ADDING_A_COUNTRY.md](docs/ADDING_A_COUNTRY.md)

**Reference implementation:** France (`fetcher/fr/`)

**What you need to know:**
- Your country's official open data source for legislation (API, XML dump, or scraping)
- Python (the pipeline is Python 3.12+)
- Basic understanding of your country's legal hierarchy (types of laws, how reforms work)

**What you produce:**
- A `fetcher/{code}/` folder with:
  - `client.py` — fetch raw data from the source
  - `parser.py` — convert it into our generic data model (`Bloque`, `Version`, `NormaMetadata`)
  - `discovery.py` — find all laws in the catalog
- Tests with fixture data

The generic layers (markdown rendering, git committing, CLI) work automatically once your parser produces the right data structures.

## Quick test

```bash
# Test your parser with just 5 laws
legalize fetch -c {code} --all --limit 5

# Dry-run: see what commits it would create
legalize bootstrap -c {code} --dry-run
```

## Development setup

```bash
# Clone
git clone https://github.com/legalize-dev/legalize-pipeline.git
cd legalize-pipeline

# Install
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check src/ tests/
```

## Code conventions

- **Python 3.12+**, type hints encouraged
- **ruff** for linting (`ruff check src/ tests/`)
- **pytest** for tests
- **Language:** existing code uses Spanish variable names and comments (the project started in Spain). New country-specific files can use English. When editing an existing file, match its conventions.
- **No frameworks for git:** we use `subprocess` for full control over `GIT_AUTHOR_DATE`

## Pull request process

1. Fork the repo
2. Create a branch (`git checkout -b add-country-de`)
3. Make your changes
4. Run tests and lint: `pytest tests/ -v && ruff check src/ tests/`
5. Submit a PR with a clear description of what the country parser does and what data source it uses

For new country PRs, include:
- Sample fixture data (a few XML/JSON files from the source)
- Tests that parse the fixtures
- A note on the data source's license/terms of use

## Questions?

Open an issue or start a discussion. We're happy to help you get started with a new country parser.
