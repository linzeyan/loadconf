# Developing github.com/linzeyan/loadconf

## Local development

Each directory with a `go.mod` is a separate Go module. Run `make` from the repository root:

```bash
make test                                 # go test -race github.com/linzeyan/loadconf/...
go vet github.com/linzeyan/loadconf/...   # needs go.work, which make test creates
```

- **`go.work` is generated, not committed**: `scripts/work.sh` writes it, and `make` rewrites it whenever a `go.mod` changes. It joins all modules into one workspace and replaces each require between them with the module's directory, so a change in `config` reaches the connectors without a release. The `go.mod` files contain no such `replace`; they require the other modules of this repository at the latest release.
- **`go mod tidy`**: it ignores `go.work` and fetches the other modules of this repository from the module proxy at the required version, so it fails until that version is tagged and pushed.
- **Memory**: compiling conn_sqlite (code translated from WebAssembly) and sink_otlp (gRPC) takes a lot of memory. On a machine with little memory, test one module at a time with `GOFLAGS=-p=1`.
- **Adding a connector**: create the module `connector/conn_xxx`. Require the other modules of this repository at the same version as the existing `go.mod` files do, without a `replace`; `make` adds the module to `go.work`. Entry points follow these rules:
  - `Open(ctx, cfg, ...)` creates the client and confirms within `ping_timeout` that it can connect. On failure, it closes the client before returning the error.
  - `New(cfg, ...)` creates the client without any network I/O. Omit it when the driver always connects while creating the client (gocql, sarama, gorm); a `New` that connects would contradict its name.
  - A client for an HTTP API holds no persistent connection, so it provides only `New`.
  - Also provide a function that converts the config to the driver's native settings, named after the driver's type (`Options`, `Config`, ...).
  - If the driver accepts a logger per client, default it to `slog.Default()`.

## Continuous integration

`.github/workflows/test.yml` runs `go vet` and `make test` on every push to `main` and on every pull request, using the Go version declared in `config/go.mod`.

## Releasing

All modules are released together at one version:

```bash
make release VERSION=v0.2.0   # scripts/release.sh v0.2.0
git push origin HEAD <the tags it prints>
```

The script:

1. Changes every require between the modules of this repository to `VERSION`.
2. Regenerates `go.work` and runs `go build` to confirm that everything compiles.
3. Commits the changes as `release VERSION`. When the requires already name `VERSION`, there is nothing to commit and it tags the current commit.
4. Tags each module `<directory>/VERSION`, for example `config/v0.2.0` and `connector/conn_redis/v0.2.0`.

The script does not push.

- **Why one tag per module**: Go finds the versions of a module in a subdirectory only through tags of the form `<directory>/vX.Y.Z`. Each module therefore needs its own tag, and one release creates all of them. OpenTelemetry-Go does the same.
- **Why one version for all modules**: a connector's `go.mod` must name a `config` version that exists. With a single version, that is the tag created on the same commit, and nobody has to track which connector version works with which `config` version. The cost is that modules without changes also get a new version, identical to the previous one.
- **Between releases**: the requires name the latest release, while `go.work` builds against the working tree. A connector that uses an unreleased change in `config` compiles here, but not for users who install it from `main`, until the next release moves the requires forward.
