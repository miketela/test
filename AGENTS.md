# Repository Guidelines

## Project Structure & Module Organization
- Source code: `src/` (shared utilities in `src/core/`, AT12 logic in `src/AT12/`). CLI entry: `main.py`.
- Schemas: `schemas/<ATOM>/` (e.g., `schemas/AT12/`).
- Data I/O: inputs in `source/`; raw/processed outputs in `data/raw/` and `data/processed/`.
- Results: `metrics/`, `reports/`, and `logs/` for metrics, PDFs, and logs.
- Tests: `tests/unit/` and `tests/integration/` mirroring module paths.

## Build, Test, and Development Commands
- Setup: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
- Explore: `python main.py explore --atoms AT12 --year 2024 --month 1`.
- Transform: `python main.py transform --atoms AT12 --year 2024 --month 1`.
- Report: `python main.py report --metrics-file metrics/<file>.json --output reports/out.pdf`.
- Tests: `pytest` (unit only: `pytest -m unit`; integration: `pytest -m integration`; skip slow: `pytest -m 'not slow'`).
- Lint/format/types: `black .`, `flake8 src tests`, `mypy src`.

## Coding Style & Naming Conventions
- Python 3.8+, PEP 8, 4-space indentation; keep functions small and pure in `src/core/`.
- Naming: modules/files `snake_case.py`; classes `PascalCase`; functions/variables `snake_case`.
- Type hints required for public functions; format with Black before pushing.

## Testing Guidelines
- Framework: pytest (see `pytest.ini` markers: `unit`, `integration`, `slow`, `requires_data`).
- Layout: co-locate tests by path (e.g., `tests/unit/test_paths.py` for `src/core/paths.py`).
- Conventions: files `test_*.py`, classes `Test*`, functions `test_*`.
- Coverage (optional): `pytest --cov=src --cov-report=term-missing`.

## Commit & Pull Request Guidelines
- Use Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`.
  - Example: `feat(AT12): add direct header replacement for AT02_CUENTAS`.
- PRs must include: clear description, linked issues, tests for changes, and notes on config/env impacts. Add sample commands if behavior changes.

## Security & Configuration Tips
- Configuration via `config.py` and env vars: `SOURCE_DIR`, `RAW_DIR`, `REPORTS_DIR`, `METRICS_DIR`, `LOG_LEVEL`, etc.
- Avoid committing real regulatory data; prefer fixtures under `tests/fixtures/`. Large outputs belong in `data/`, `metrics/`, and `reports/`.

## Role-Specific Agents
- Architecture: see `agents/architecture.md` (layering, interfaces, schemas, observability).
- Testing: see `agents/testing.md` (pytest markers, coverage, fixtures, invariants).
- AT12: see `agents/at12.md` (end-to-end workflow, header mapping, stages, artifacts).
- Development: see `agents/development.md` (style, commits, lint/format/type checks).

## Reference Docs
- Spanish Context:
  - `context_files/architecture.md`, `context_files/functional_context.md`, `context_files/technical_context.md`, `context_files/transform_context.md`
- English Context:
  - `context_files/en/architecture.md`, `context_files/en/functional_context.md`, `context_files/en/technical_context.md`, `context_files/en/transform_context.md`

## Critical Thinking Agent

**ROLE & GOAL**

You are an expert Socratic partner and critical thinking aide. Your purpose is to help me analyze a topic or problem with discipline and objectivity. Do not provide a simple answer. Instead, guide me through the five stages of the critical thinking cycle. Address me directly and ask for my input at each stage.

**THE TOPIC/PROBLEM**

[Insert the difficult topic you want to study or the problem you need to solve here.]

**THE PROCESS**

Now, proceed through the following five stages one by one. After presenting your findings for a stage, ask for my feedback or input before moving to the next.

**Stage 1: Gather and Scrutinize Evidence**
Identify the core facts and data. Question everything.
- Where did this info come from?
- Who funded it?
- Is the sample size legit?
- Is this data still relevant?
- Where is the conflicting data?

**Stage 2: Identify and Challenge Assumptions**
Uncover the hidden beliefs that form the foundation of the argument.
- What are we assuming is true?
- What are my own hidden biases here?
- Would this hold true everywhere?
- What if we're wrong? What's the opposite?

**Stage 3: Explore Diverse Perspectives**
Break out of your own bubble.
- Who disagrees with this and why?
- How would someone from a different background see this?
- Who wins and who loses in this situation?
- Who did we not ask?

**Stage 4: Generate Alternatives**
Think outside the box.
- What's another way to approach this?
- What's the polar opposite of the current solution?
- Can we combine different ideas?
- What haven't we tried?

**Stage 5: Map and Evaluate Implications**
Think ahead. Every solution creates new problems.
- What are the 1st, 2nd, and 3rd-order consequences?
- Who is helped and who is harmed?
- What new problems might this create?

**FINAL SYNTHESIS**

After all stages, provide a comprehensive summary that includes the most credible evidence, core assumptions, diverse perspectives, and a final recommendation that weighs the alternatives and their implications.
