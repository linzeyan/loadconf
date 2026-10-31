# Developing github.com/linzeyan/loadconf

## Local development

Each directory with a `go.mod` is a separate Go module. Run `make` from the repository root:

```bash
make lint   # go vet in every module, and the replace check below
make test   # go test -race in every module, several modules at a time
```

- **Requires between modules are replaced with directories**: each `go.mod` requires the other modules of this repository at the latest release and replaces them with their directories, e.g. `replace github.com/linzeyan/loadconf/config => ../../config`. A change in `config` therefore reaches the connectors without a release, and `go mod tidy` works in any module before a version is tagged. Users of the modules ignore `replace`, so for them the requires decide the version. OpenTelemetry-Go works the same way.
- **`scripts/replaces.sh`** adds the missing replaces; run it, then `go mod tidy`, after adding a module or a require between modules. Without a replace, Go fetches that module from the proxy and tests the released code instead of the working tree; `make lint` fails when a `go.sum` lists a module of this repository, which is the sign of it.
- **`go.work` is optional and not committed**: gopls finds references across modules only inside a workspace, so for the editor you may run `go work init && go work use -r .` at the root. `make` sets `GOWORK=off`, so it always checks each module as its `go.mod` describes it.
- **Memory**: compiling conn_sqlite (code translated from WebAssembly) and sink_otlp (gRPC) takes a lot of memory. `make` runs half as many modules at once as there are CPUs; on a machine with little memory, run one module and one package at a time with `JOBS=1 GOFLAGS=-p=1 make test`.
- **Adding a connector**: create the module `connector/conn_xxx`. Require the other modules of this repository at the same version as the existing `go.mod` files do, then run `scripts/replaces.sh` and `go mod tidy`. Entry points follow these rules:
  - `Open(ctx, cfg, ...)` creates the client and confirms within `ping_timeout` that it can connect. On failure, it closes the client before returning the error.
  - `New(cfg, ...)` creates the client without any network I/O. Omit it when the driver always connects while creating the client (gocql, sarama, gorm); a `New` that connects would contradict its name.
  - A client for an HTTP API holds no persistent connection, so it provides only `New`.
  - Also provide a function that converts the config to the driver's native settings, named after the driver's type (`Options`, `Config`, ...).
  - If the driver accepts a logger per client, default it to `slog.Default()`.

## Continuous integration

`.github/workflows/test.yml` runs `make lint` and `make test` on every push to `main` and on every pull request, using the Go version declared in `config/go.mod`.

## Releasing

All modules are released together at one version:

```bash
make release VERSION=v0.2.0   # scripts/release.sh v0.2.0
git push origin HEAD <the tags it prints>
```

The script:

1. Changes every require between the modules of this repository to `VERSION`.
2. Runs `go build` in every module to confirm that everything compiles. The replaces make this work before the tags exist.
3. Commits the changes as `release VERSION`. When the requires already name `VERSION`, there is nothing to commit and it tags the current commit.
4. Tags each module `<directory>/VERSION`, for example `config/v0.2.0` and `connector/conn_redis/v0.2.0`.

The script does not push.

- **Why one tag per module**: Go finds the versions of a module in a subdirectory only through tags of the form `<directory>/vX.Y.Z`. Each module therefore needs its own tag, and one release creates all of them. OpenTelemetry-Go does the same.
- **Why one version for all modules**: a connector's `go.mod` must name a `config` version that exists. With a single version, that is the tag created on the same commit, and nobody has to track which connector version works with which `config` version. The cost is that modules without changes also get a new version, identical to the previous one.
- **Between releases**: the requires name the latest release, while the replaces build against the working tree. A connector that uses an unreleased change in `config` compiles here, but not for users who install it from `main`, until the next release moves the requires forward.
