# AGENTS.md - agfs-server

## Scope

- This file applies to everything under `agfs-server/`.
- `agfs-server` is the Go HTTP server and plugin runtime for AGFS.
- Prefer this file over the repo-root `AGENTS.md` when the two overlap.

## High-level architecture

- `cmd/server/`: binary entrypoint, built-in plugin registration, config bootstrapping.
- `pkg/filesystem/`: core filesystem interfaces, flags, typed errors, path helpers, handle abstractions.
- `pkg/mountablefs/`: mount router, plugin registration, handle indirection, symlink routing.
- `pkg/plugin/`: plugin interfaces, loader, config validation helpers, WASM/native plugin support.
- `pkg/plugins/`: built-in filesystem/service plugins such as `queuefs`, `memfs`, `s3fs`, `sqlfs`, `vectorfs`.
- `pkg/handlers/`: HTTP API handlers and traffic monitoring.
- `docs/concepts/`: concept-level design notes and semantic contracts for bigger features; start here when a task references a design or RFC-like behavior.
- `scripts/run_failpoint_tests.py`: the only supported entrypoint for failpoint tests.

## Build and run commands

- Build server: `make build`
- Run built server: `make run`
- Run in dev mode: `make dev`
- Install binary: `make install`
- Download/tidy deps: `make deps`
- Update deps: `make deps-update`
- Lint: `make lint`
- Clean build artifacts: `make clean`

## Test commands

- Full unit test suite: `make test`
- Direct full suite: `go test -v ./...`
- Single package: `go test -v ./pkg/plugins/queuefs/...`
- Single package without cache: `go test -v ./pkg/plugins/queuefs -count=1`
- Single test by exact name: `go test -v ./pkg/plugins/queuefs -run '^TestQueueFSSQLite$' -count=1`
- Single subtest pattern: `go test -v ./pkg/plugins/queuefs -run 'TestQueueFSSQLite/.*' -count=1`
- PostgreSQL integration tests: `PG_TEST_DSN="postgresql://postgres@127.0.0.1:5432/postgres?sslmode=disable" make integration-test-pg`
- TiDB integration tests: `TIDB_TEST_DSN='root@tcp(127.0.0.1:4000)/queuedb?charset=utf8mb4&parseTime=True' make integration-test-tidb`
- Failpoint tests: `make test-failpoint`

## Testing rules and cautions

- Do not run `make test` and `make test-failpoint` concurrently.
- Failpoint tests must go through `scripts/run_failpoint_tests.py`; do not substitute plain `go test` for `*_failpoint_test.go` files.
- Integration tests are env-gated; only run PostgreSQL or TiDB suites when the corresponding DSN is set.
- Prefer the narrowest `go test` target first, then widen to package or full-suite coverage.
- Use `-count=1` when a package relies on temp DBs, failpoints, or mutable external state.

## Plugin development workflow

- New built-in plugins usually live in `pkg/plugins/<name>/`.
- Plugins implement `plugin.ServicePlugin` from `pkg/plugin/plugin.go`.
- Required methods are `Name`, `Validate`, `Initialize`, `GetFileSystem`, `GetReadme`, `GetConfigParams`, and `Shutdown`.
- The server registers built-in plugins in `cmd/server/main.go`; add factory wiring there when introducing a new built-in plugin.
- Keep plugin docs practical: `GetReadme()` should explain the virtual filesystem layout and expected workflow.
- Validate config early and fail with clear errors before initialization does expensive work.

## Go style and conventions

- Follow standard Go formatting (`gofmt`/`goimports`) and keep imports grouped cleanly.
- Package names are short and lowercase; exported names use `PascalCase`, unexported helpers use `camelCase`.
- Keep comments on exported types, functions, interfaces, and constants useful and behavior-oriented.
- Prefer constructor/helper names like `NewMountableFS`, `NewQueueFSPlugin`, and `NewNotFoundError`.
- Favor small focused helpers over giant methods, especially in plugins and mount routing code.
- Use `t.Helper()` in test helper functions.

## Error handling

- Prefer typed filesystem errors from `pkg/filesystem/errors.go` over ad hoc string-only errors.
- Use `errors.Is()` compatibility by returning wrapped or typed errors where possible.
- Wrap underlying causes with `fmt.Errorf("context: %w", err)` instead of discarding the original error.
- Return domain-specific errors such as `filesystem.NewAlreadyExistsError`, `filesystem.NewNotFoundError`, or `filesystem.NewPermissionDeniedError` when they match the failure.
- Validate arguments and config explicitly; `pkg/plugin/config/validation.go` already provides reusable helpers.

## Filesystem and path expectations

- Normalize mount paths and user-supplied paths consistently; `mountablefs` already normalizes mount targets before mutation.
- Reuse the existing interfaces in `pkg/filesystem/filesystem.go` instead of inventing plugin-specific variants.
- Respect write/open flag semantics defined in `pkg/filesystem/filesystem.go`.
- If a plugin needs parent or root filesystem access, follow the existing setter-interface patterns used in `pkg/mountablefs/mountablefs.go`.

## Testing conventions

- Put tests next to the package they verify using standard `*_test.go` names.
- Table-driven tests are common and preferred for parsing, formatting, and validation logic.
- For filesystem behavior, add regression tests that exercise the public `filesystem.FileSystem` methods, not just private helpers.
- For plugin tests, include helpers that build configured test instances and clean up resources with `t.Cleanup`.
- Use temp directories or temp databases for local state; do not rely on shared machine-global paths.

## Common patterns worth reusing

- Config validation helpers from `pkg/plugin/config/validation.go` for strings, ints, bools, arrays, and size parsing.
- Typed sentinel/structured errors from `pkg/filesystem/errors.go`.
- Package-local test constructors such as `newTestQueueFS` and helper readers such as `mustReadAll`.
- Cleanup via `t.Cleanup` for plugin backends, temp files, and transient services.

## When changing HTTP/API behavior

- Check `cmd/server/main.go`, `pkg/handlers/`, and `api.md` together.
- Preserve existing API paths and semantics unless the change explicitly requires a contract update.
- If you change externally visible behavior, update `README.md` or `api.md` in the same change.

## Practical defaults for agents

- Use `make` targets when available instead of spelling out the equivalent `go` commands.
- Start validation with the nearest package test, then run `make test` if the scope is broad.
- For `queuefs`, read `pkg/plugins/queuefs/README.md` before changing integration-test behavior.
- For failpoint-related work, read `scripts/run_failpoint_tests.py` before editing test invocation logic.
