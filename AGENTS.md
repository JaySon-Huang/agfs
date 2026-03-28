# AGENTS.md

## Scope and intent

- This repository is a multi-language AGFS monorepo with separate Go, Python, and small React/Vite components.
- There is no single root build entrypoint; run commands from the relevant subproject directory.
- If you work inside `agfs-shell/`, also follow the stricter local rules in `agfs-shell/AGENTS.md`.
- No repository-level Cursor rules were found in `.cursor/rules/` or `.cursorrules`.
- No repository-level Copilot rules were found in `.github/copilot-instructions.md`.

## Repository map

- `agfs-server/`: main Go server and plugin system.
- `agfs-fuse/`: Go FUSE client for Linux.
- `agfs-sdk/go/`: Go SDK.
- `agfs-sdk/python/`: Python SDK (`pyagfs`).
- `agfs-shell/`: Python shell, parser, command runtime, plus `webapp/`.
- `agfs-mcp/`: Python MCP server built on `pyagfs`.

## Preferred workflow

- Start by identifying the subproject you are changing; do not assume commands from one module apply to another.
- Prefer the documented `make`, `uv`, `go test`, and `npm` commands already present in the repo.
- For Go work, use package-scoped `go test` before broader suites.
- For Python work, use `uv` instead of bare `pip` or `python` when project config already does so.
- For `agfs-shell`, run lint/type/test checks before finishing because that package has the strongest local quality workflow.

## Build, lint, and test commands

### Root

- There is no root `Makefile`, workspace manifest, or unified test runner.
- CI is split across `.github/workflows/agfs-server.yml`, `.github/workflows/agfs-go.yml`, `.github/workflows/agfs-shell.yml`, and `.github/workflows/agfs-shell-lint.yml`.

### `agfs-server/` (Go)

- Build: `make build`
- Run dev server: `make dev`
- Run built binary: `make run`
- Install binary: `make install`
- Unit tests: `make test`
- Lint: `make lint`
- Failpoint tests: `make test-failpoint`
- PostgreSQL integration tests: `PG_TEST_DSN="postgresql://..." make integration-test-pg`
- TiDB integration tests: `TIDB_TEST_DSN='root@tcp(127.0.0.1:4000)/queuedb?charset=utf8mb4&parseTime=True' make integration-test-tidb`
- Single package: `go test -v ./pkg/plugins/queuefs/...`
- Single test by name: `go test -v ./pkg/plugins/queuefs -run '^TestQueueFSSQLite$' -count=1`
- Single subcase pattern: `go test -v ./pkg/plugins/queuefs -run 'TestQueueFSSQLite/.*' -count=1`
- Important: do not run `make test` and `make test-failpoint` concurrently; failpoint instrumentation rewrites the tree.

### `agfs-fuse/` (Go)

- Build: `make build`
- Install: `make install`
- Tests: `make test`
- Direct build: `go build -o build/agfs-fuse ./cmd/agfs-fuse`
- Single package: `go test -v ./pkg/cache`
- Single test: `go test -v ./pkg/cache -run '^TestName$'`

### `agfs-sdk/go/` (Go)

- Tests: `go test -v ./...`
- Single package: `go test -v ./...` scoped to the target package path.
- Single test: `go test -v ./path/to/pkg -run '^TestName$'`

### `agfs-shell/` (Python)

- Sync dependencies: `uv sync --all-extras --dev`
- Build portable distribution: `make build`
- Install locally: `make install`
- Full tests: `make test` or `uv run pytest tests/`
- Verbose tests with coverage: `uv run pytest tests/ -v --cov=agfs_shell --cov-report=xml --cov-report=term`
- Single test file: `uv run pytest tests/test_parser.py`
- Single test function: `uv run pytest tests/test_parser.py::test_function_name -v`
- Single test class/method: `uv run pytest tests/test_process.py::TestProcess::test_method -v`
- Black check: `uv run black --check agfs_shell/ tests/`
- isort check: `uv run isort --check-only agfs_shell/ tests/`
- Ruff: `uv run ruff check agfs_shell/ tests/`
- Mypy: `uv run mypy agfs_shell/ --ignore-missing-imports`
- All local quality checks: `./scripts/quality-check.sh`

### `agfs-shell/webapp/` (React + Vite)

- Install deps: `npm install`
- Dev server: `npm run dev`
- Production build: `npm run build`
- Preview build: `npm run preview`
- No frontend test runner is currently configured in `package.json`.

### `agfs-mcp/` (Python)

- Editable install: `uv pip install -e .`
- Run server: `agfs-mcp`
- Alternate run with env: `AGFS_SERVER_URL=http://localhost:8080 agfs-mcp`
- No dedicated lint/test commands are defined in this package today.

### `agfs-sdk/python/` (Python)

- Editable install: `pip install -e .` or `uv pip install -e .`
- Optional dev extras exist in `pyproject.toml`, but no checked-in test runner command is documented beyond standard `pytest` usage.
- If you add tests here, prefer `pytest` and align style with the other Python packages.

## Testing expectations

- Run the narrowest relevant test first, then the broader package suite if the narrow test passes.
- For Go tests that touch databases or failpoints, respect the documented env-gated and serialized workflows.
- For Python changes in `agfs-shell`, at minimum run the targeted pytest selection; ideally run `./scripts/quality-check.sh` before finishing.
- When adding new command behavior in `agfs-shell`, add or update pytest coverage in `agfs-shell/tests/`.
- When adding new Go plugin behavior, prefer package-local tests near the plugin or filesystem package.

## Local instruction files worth honoring

- `agfs-server/AGENTS.md`: source of truth for `agfs-server` commands and conventions.
- `agfs-shell/AGENTS.md`: contains shell-specific architecture notes, command patterns, and anti-patterns.
- `agfs-shell/CONTRIBUTING.md`: source of truth for the Python shell test and quality workflow.
- `agfs-server/README.md`: source of truth for failpoint and integration-test execution.
- `agfs-server/pkg/plugins/queuefs/README.md`: best reference for queuefs-specific targeted test commands.

## Practical defaults for agents

- If the task is in `agfs-shell/`, assume `uv`, pytest, Black, isort, Ruff, and mypy are the preferred tools.
- If the task is in `agfs-server/`, assume `make` targets are preferred over hand-written shell equivalents.
- If the task is in Go code outside `agfs-server/`, use `go test -v ./...` and narrow with `-run` when iterating.
- If the task is in `webapp/`, use `npm run build` to catch obvious regressions because no tests are configured.
- If you cannot find a formal command for a subproject, state that explicitly rather than inventing a repository standard that does not exist.
