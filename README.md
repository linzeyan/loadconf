# loadconf

English | [繁體中文](README.zh-TW.md)

Declare a struct and write a config file; get back validated settings, a logger, and each driver's own client (`*sql.DB`, `*gorm.DB`, `redis.UniversalClient`, `*kgo.Client`, …).

Every connector is a separate Go module, so a service that only uses Redis does not depend on the Kafka or Oracle drivers. Requires Go 1.26 or later.

## Contents

- [Install](#install)
- [Examples](#examples)
  1. [One config file](#1-one-config-file)
  2. [One file per environment](#2-one-file-per-environment)
  3. [Environment variables and secrets](#3-environment-variables-and-secrets)
  4. [Several databases](#4-several-databases)
  5. [Configuration in etcd](#5-configuration-in-etcd)
  6. [Driver settings](#6-driver-settings)
  7. [Read replicas](#7-read-replicas)
  8. [Logging to several outputs](#8-logging-to-several-outputs)
  9. [Hot reload and reconnecting](#9-hot-reload-and-reconnecting)
  10. [Configuration for each layer](#10-configuration-for-each-layer)
- [Reference](#reference)
  - [Sources and precedence](#sources-and-precedence)
  - [Which sources read ENV](#which-sources-read-env)
  - [Environment variable names](#environment-variable-names)
  - [Struct tags](#struct-tags)
  - [Open and New](#open-and-new)
  - [Connectors](#connectors)
  - [Logger](#logger)
- [Versioning](#versioning)

## Install

```bash
# All modules are released together under one version: request the same version for each.
go get github.com/linzeyan/loadconf/config@v0.1.0 \
  github.com/linzeyan/loadconf/logger@v0.1.0 \
  github.com/linzeyan/loadconf/connector/conn_gorm@v0.1.0
```

The import path is the module path; the package name is its last element.

| Import path | Provides |
|---|---|
| `github.com/linzeyan/loadconf/config` | Loading, and the settings types of every connection (`config.MySQL`, `config.Redis`, …) |
| `github.com/linzeyan/loadconf/config/etcd` | etcd sources; a separate module, so services without etcd do not depend on its client |
| `github.com/linzeyan/loadconf/logger` | An slog logger and its outputs |
| `github.com/linzeyan/loadconf/logger/logger_zap`, `…/logger_zerolog`, `…/sink_otlp` | The same configuration as a zap or zerolog logger; the OTLP output |
| `github.com/linzeyan/loadconf/connector/conn_<driver>` | One module per driver; see [Connectors](#connectors) |

## Examples

Each example adds one thing to an earlier one. The first line of each names the change.

### 1 One config file

`config.yaml`:

```yaml
# A key is the snake_case name of its field, with product names kept whole:
# MySQL -> mysql, MaxOpenConns -> max_open_conns. Matching ignores case.
port: 8080
mysql:
  host: 127.0.0.1         # port: 3306 unless set
  user: app
  password: dev-only
  database: shop
  max_open_conns: 20      # unset or 0 keeps database/sql's default: no limit
  conn_max_lifetime: 5m
```

`main.go`:

```go
package main

import (
	"context"
	"log"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
)

// Fields need no tags.
type Config struct {
	Port  int `default:"8080"` // used when no source sets port
	MySQL config.MySQL
}

func main() {
	ctx := context.Background()
	// The extension selects the format: .yaml, .yml, .json or .toml.
	// Load merges the sources, applies defaults, decodes, then validates. It
	// reports every problem at once, each with its key path:
	//   config: decode: mysql.max_open_conns: invalid integer "many"
	//   mysql.port: invalid integer "abc"
	// A key that matches no field, usually a typo, only logs a warning:
	//   WARN config keys match no field and are ignored source=file(config.yaml) keys=[mysql.hots]
	// With config.WithStrict() it is an error instead.
	cfg, err := config.Load[Config](ctx, config.From(config.File("config.yaml")))
	if err != nil {
		log.Fatal(err)
	}

	// OpenMySQL builds the pool and pings it within ping_timeout (default
	// 3s). If the ping fails, it closes the pool and returns the error.
	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn_gorm.Close(db)

	log.Printf("listening on :%d", cfg.Port)
}
```

### 2 One file per environment

**Changes from example 1:** `config.File("config.yaml")` becomes `config.Profile("app")`.

```
configs/
├── app.yaml        # shared by every environment; may be absent
├── app.dev.yaml    # ENV=dev, and ENV unset
└── app.prod.yaml   # ENV=prod
```

```yaml
# configs/app.yaml
port: 8080
mysql:
  user: app
  database: shop
  max_open_conns: 20
```

```yaml
# configs/app.dev.yaml: merged into app.yaml key by key
mysql:
  host: 127.0.0.1
  password: dev-only
```

```yaml
# configs/app.prod.yaml: the password comes from the environment (example 3)
mysql:
  host: db.prod.internal
  max_open_conns: 100     # overrides the 20 of app.yaml; user and database are kept
```

```go
package main

import (
	"context"
	"log"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
)

type Config struct {
	Port  int `default:"8080"`
	MySQL config.MySQL
}

func main() {
	ctx := context.Background()
	// Reads configs/app.yaml, then configs/app.$ENV.yaml over it. Each load
	// logs the files it read:
	//   INFO config loaded sources="[file(configs/app.yaml) file(configs/app.dev.yaml)]"
	cfg, err := config.Load[Config](ctx, config.From(config.Profile("app")))
	if err != nil {
		log.Fatal(err)
	}

	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn_gorm.Close(db)

	log.Printf("listening on :%d", cfg.Port)
}
```

| Run with | Files read | `mysql.host` | `mysql.max_open_conns` |
|---|---|---|---|
| `ENV` unset, or `ENV=dev` | `app.yaml`, then `app.dev.yaml` | `127.0.0.1` | 20 |
| `ENV=prod` | `app.yaml`, then `app.prod.yaml` | `db.prod.internal` | 100 |
| `ENV=stage` | none; `Load` fails: `no config file app.stage{.yaml,.yml,.json,.toml} in ./configs` | | |
| `CONFIG_PATH=/etc/app/app.yaml` | that file only; `ENV` is ignored | from that file | from that file |

`Profile` options:

```go
config.Profile("app", config.ProfileDir("/etc/app"))              // directory of the files; default ./configs
config.Profile("app", config.ProfileEnvs("dev", "stage", "prod")) // any other ENV fails: unsupported ENV="prdo" (want one of dev, stage, prod)
config.Profile("app", config.ProfileEnvVar("APP_ENV"))            // read APP_ENV instead of ENV
config.Profile("app", config.ProfileDefaultEnv("local"))          // environment when the variable is unset; default dev
config.Profile("app", config.ProfilePathEnvVar("APP_CONFIG"))     // variable naming a single file; default CONFIG_PATH
```

### 3 Environment variables and secrets

**Changes from example 2:** `config.DotEnv` and `config.Env` join the sources, and `PaymentKey` is a `config.Secret`.

```go
package main

import (
	"context"
	"log"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
)

type Config struct {
	// env:"PORT" reads $PORT after all sources, so the port a platform sets
	// wins over the files and over APP_PORT.
	Port int `default:"8080" env:"PORT"`
	// The password fields of config.MySQL are config.Secret already.
	MySQL config.MySQL
	// A non-empty Secret prints as ****** under every fmt verb; Value()
	// returns the real string.
	PaymentKey  config.Secret
	PaymentHost string `default:"https://pay.example.com"`
}

func main() {
	ctx := context.Background()
	// A later source overrides an earlier one, key by key.
	cfg, err := config.Load[Config](ctx, config.From(
		config.Profile("app"),
		// .env in the working directory, for local development. Without
		// DotEnvOptional, a missing file is an error. Names map to keys as
		// in Env below.
		config.DotEnv(".env", "APP", config.DotEnvOptional()),
		// APP_MYSQL__PASSWORD sets mysql.password: "__" separates levels, a
		// single "_" stays in the key. An empty value counts as unset.
		config.Env("APP"),
	))
	if err != nil {
		log.Fatal(err)
	}
	// Prints ... Password:****** ... PaymentKey:******
	log.Printf("config: %+v", *cfg)

	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn_gorm.Close(db)

	pay(cfg.PaymentHost, cfg.PaymentKey.Value())
}

func pay(host, key string) {}
```

```bash
# .env: local development only; keep it out of version control
APP_MYSQL__PASSWORD=dev-only
APP_PAYMENT_KEY=test-key
```

```bash
# Production environment
ENV=prod                      # selects configs/app.prod.yaml
APP_MYSQL__PASSWORD=s3cret    # mysql.password
APP_PAYMENT_KEY=live-key      # payment_key
PORT=9090                     # the env tag of Port; APP_PORT=9090 also works, but PORT wins if both are set
```

### 4 Several databases

**Changes from example 3:** `config.MySQL` becomes `config.Named[config.MySQL]`, and the code picks instances by name.

```yaml
# configs/app.yaml
mysql:                   # a map: each key is an instance name
  orders:
    host: db1
    user: app
    database: orders
  report:
    host: db2
    user: report
    database: report
redis:                   # or a list whose items carry a name
  - name: cache
    addrs: ["r1:6379"]
  - name: session
    addrs: ["r2:6379", "r3:6379"]   # more than one address: a cluster client
# Inside a flow mapping {...}, quote host:port: {addrs: [r1:6379]} parses
# r1:6379 as a map. Block style, as above, needs no quotes.
```

```go
package main

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
	"github.com/linzeyan/loadconf/connector/conn_redis"
)

type Config struct {
	// Named holds instances by name. With exactly one database,
	// config.MySQL is enough (examples 1 to 3).
	MySQL config.Named[config.MySQL]
	Redis config.Named[config.Redis]
}

func main() {
	ctx := context.Background()
	cfg, err := config.Load[Config](ctx, config.From(config.Profile("app"), config.Env("APP")))
	if err != nil {
		log.Fatal(err)
	}

	// MustGet panics when the name is missing, which suits start-up.
	// Names match case-insensitively.
	orders, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("orders"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn_gorm.Close(orders)

	// Get reports whether the instance exists, for optional ones.
	if reportCfg, ok := cfg.MySQL.Get("report"); ok {
		report, err := conn_gorm.OpenMySQL(ctx, reportCfg)
		if err != nil {
			log.Fatal(err)
		}
		defer conn_gorm.Close(report)
	}

	// All yields every instance, sorted by name.
	caches := map[string]redis.UniversalClient{}
	for name, rc := range cfg.Redis.All() {
		c, err := conn_redis.Open(ctx, rc)
		if err != nil {
			log.Fatalf("redis %s: %v", name, err)
		}
		defer c.Close()
		caches[name] = c
	}
}
```

Sources merge instance by instance, so an environment variable can change one field of one instance, or add an instance:

```bash
APP_MYSQL__REPORT__PASSWORD=s3cret   # report keeps its other fields
APP_MYSQL__ARCHIVE__HOST=10.0.0.3    # a new instance "archive", defined by the environment alone
```

### 5 Configuration in etcd

**Changes from example 4:** the configuration is read from etcd. The program must know where etcd is before it can read from it, so it loads in two stages.

```go
package main

import (
	"cmp"
	"context"
	"log"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/config/etcd"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
)

// Bootstrap is what the process needs to reach etcd.
type Bootstrap struct {
	Etcd EtcdClient
}

type EtcdClient struct {
	Endpoints []string `validate:"min=1"`
	Username  string
	Password  config.Secret
}

type Config struct {
	Port  int `default:"8080"`
	MySQL config.Named[config.MySQL]
}

func main() {
	ctx := context.Background()
	// ENV selects only Profile's file names. etcd keys are read exactly as
	// written, so the environment has to be part of the key. cmp.Or applies
	// Profile's rule (unset means dev), so files and etcd agree.
	env := cmp.Or(os.Getenv("ENV"), "dev")

	// Stage 1, from the environment: APP_ETCD__ENDPOINTS=etcd1:2379,etcd2:2379
	boot, err := config.Load[Bootstrap](ctx, config.From(config.Env("APP")))
	if err != nil {
		log.Fatal(err)
	}
	// With a Username, clientv3.New authenticates at once and fails if etcd
	// is unreachable. Without one, New does no I/O and the first read fails.
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   boot.Etcd.Endpoints,
		Username:    boot.Etcd.Username,
		Password:    boot.Etcd.Password.Value(),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// Stage 2. Each etcd read gives up after 5s (etcd.Timeout changes it):
	//   config: load etcd-key(/order-service/prod/config.yaml): context deadline exceeded
	// A missing key fails too, unless the source has etcd.Optional():
	//   config: load etcd-key(/order-service/prod/config.yaml): key /order-service/prod/config.yaml not found
	cfg, err := config.Load[Config](ctx, config.From(
		// One key holding the whole document; the .yaml suffix sets the format.
		etcd.Key(cli, "/order-service/"+env+"/config.yaml"),
		config.Env("APP"), // still overrides single fields
	))
	if err != nil {
		log.Fatal(err)
	}

	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("orders"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn_gorm.Close(db)
}
```

Three ways to lay out the keys:

| Source | Stored in etcd | Suits |
|---|---|---|
| `etcd.Key(cli, "/order-service/prod/config.yaml")` | the whole YAML document in one key | reviewing and changing the configuration as one document |
| `etcd.Key(cli, "/sport/prod", etcd.Format("json"))` | the whole JSON document in one key without a suffix | an existing `/{app}/{env}` JSON layout; keys such as `"MySQL"` match too |
| `etcd.Prefix(cli, "/order-service/prod/")` | one key per field, as below | fields maintained by different people or tools |

```
# etcd.Prefix: the key after the prefix, split on "/", is the key path.
# Values are strings, converted as environment variables are.
/order-service/prod/port                    9090
/order-service/prod/mysql/orders/host       db1
/order-service/prod/mysql/orders/password   s3cret
/order-service/prod/redis/cache/addrs       r1:6379,r2:6379
```

```go
// Shared and per-environment layers, like app.yaml and app.$ENV.yaml:
cfg, err := config.Load[Config](ctx, config.From(
	etcd.Prefix(cli, "/order-service/common/"),  // every environment
	etcd.Prefix(cli, "/order-service/"+env+"/"), // this environment; overrides common
	config.Env("APP"),
))
```

Both source kinds are watchable: under `Loader.Watch` (example 9), a change to the key or under the prefix triggers a reload. The watch resumes from the revision of the last read, so no change in between is missed.

### 6 Driver settings

**Changes from example 4:** driver settings beyond the typed fields go in `options` or `params` in the config file; only functions and hooks go in code.

```yaml
# configs/app.yaml
redis:
  addrs: ["r1:6379"]
  # options decode into the driver's own struct, here redis.UniversalOptions,
  # with snake_case field names. A misspelled key, a wrong type, or a key that
  # has its own field makes Open fail:
  #   unknown options: pool_sise
  #   options.password: set by its own config key, not in options
  # The keys follow the driver version: if an upgrade renames a field, Open
  # fails the same way instead of silently ignoring the setting.
  options: {pool_size: 50, read_timeout: 2s}

kafka:
  brokers: ["k1:9092"]
  # Read only by conn_kafka_sarama: sarama.Config, nested structs as nested maps.
  # conn_kafka_franz fails when options is set; give it kgo options in code.
  options:
    producer: {compression: zstd, retry: {max: 5}}

postgres:
  main:
    host: pg
    # params are appended to the DSN as given (MySQL, PostgreSQL, SQL Server,
    # Oracle, Trino, MongoDB). Keys from environment variables arrive in lower
    # case, so write case-sensitive ones, such as MySQL's parseTime, in a file.
    params: {sslmode: require, application_name: order-service}
```

```go
package main

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_kafka_franz"
	"github.com/linzeyan/loadconf/connector/conn_redis"
)

type Config struct {
	Redis    config.Redis
	Kafka    config.Kafka
	Postgres config.Named[config.Postgres] // opened as in example 4
}

func main() {
	ctx := context.Background()
	cfg, err := config.Load[Config](ctx, config.From(config.Profile("app"), config.Env("APP")))
	if err != nil {
		log.Fatal(err)
	}

	// A function cannot be written in a file, so it is passed in code. Code
	// options run after the file's settings and override them.
	rdb, err := conn_redis.Open(ctx, cfg.Redis, func(o *redis.UniversalOptions) {
		o.OnConnect = func(ctx context.Context, cn *redis.Conn) error { return nil }
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

	// franz-go is configured through functions, so its settings are kgo options.
	kc, err := conn_kafka_franz.Open(ctx, cfg.Kafka, kgo.DefaultProduceTopic("events"))
	if err != nil {
		log.Fatal(err)
	}
	defer kc.Close()
}
```

```bash
go doc github.com/linzeyan/loadconf/config.Redis   # every field of a settings type
```

### 7 Read replicas

**Changes from example 4:** a second instance is attached as a read replica with `conn_gorm.WithMySQLReplicas` (`WithPostgresReplicas` for PostgreSQL).

```yaml
mysql:
  orders: &orders           # &orders names this mapping
    host: db-primary
    user: app
    database: shop
    max_open_conns: 50
  orders_replica:
    <<: *orders             # copies every key of orders,
    host: db-replica        # then overrides these two
    max_open_conns: 100
```

```bash
# The merge above happens inside the file. Environment variables override
# instances by name, so each instance needs its own password.
APP_MYSQL__ORDERS__PASSWORD=s3cret
APP_MYSQL__ORDERS_REPLICA__PASSWORD=s3cret
```

```go
package main

import (
	"context"
	"log"

	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
)

type Config struct {
	MySQL config.Named[config.MySQL]
}

type Order struct {
	ID     uint
	Status string
}

func main() {
	ctx := context.Background()
	cfg, err := config.Load[Config](ctx, config.From(config.Profile("app"), config.Env("APP")))
	if err != nil {
		log.Fatal(err)
	}

	// Each replica is opened like the primary: its own pool settings,
	// parseTime on, and WithSQLOptions and WithMySQLDialector applied. If
	// any fails, OpenMySQL closes what it opened and returns the error.
	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("orders"),
		conn_gorm.WithMySQLReplicas(cfg.MySQL.MustGet("orders_replica")))
	if err != nil {
		log.Fatal(err)
	}
	// Close closes the replica pools too; db.DB() returns only the primary's.
	defer conn_gorm.Close(db)

	order := Order{Status: "new"}
	db.Create(&order)
	db.Find(&[]Order{})
	// A replica may not have the row yet: pin a read that must see a write.
	db.Clauses(dbresolver.Write).First(&order, order.ID)
	db.Transaction(func(tx *gorm.DB) error {
		return tx.Model(&order).Update("status", "paid").Error
	})
}
```

| Operation | Runs on |
|---|---|
| `Create`, `Save`, `Update`, `Delete`, `Exec` | the primary |
| `Find`, `First`, `Count`, `Scan` | a replica, chosen at random |
| `Raw("SELECT …")` | a replica; the primary if the statement ends with `FOR UPDATE` |
| any query inside `Transaction`, reads included | the primary |
| a read with `Clauses(dbresolver.Write)` | the primary |

### 8 Logging to several outputs

**Changes from example 3:** `config.Log` and `logger.New` are added. One log call writes the record to every output whose level admits it.

```yaml
# configs/app.yaml (mysql as in example 2)
log:
  level: info               # the minimum for every output
  service: order-service    # adds service=order-service to every record
  # stack_level: error      # default: records at error and above carry a stack field
  outputs:
    stdout:                 # an output's name is also its type, unless type is set
      format: text          # stdout, stderr and file may use text; other outputs always get JSON
    file:
      path: /var/log/order-service/app.log
      max_size: 100MiB      # rotate beyond this size (default 100MiB)
      max_backups: 7
      compress: true
    errors:                 # a second file output, so its type is set explicitly
      type: file
      path: /var/log/order-service/error.log
      level: error          # this output's minimum; the higher of it and log.level applies
    elasticsearch:
      addresses: ["https://es:9200"]
      index: "logs-{service}-{date:2006.01.02}"
      level: warn
```

```yaml
# configs/app.dev.yaml: outputs merge by name, so only the differences are written
log:
  level: debug
  outputs:
    file: {path: logs/app.log}         # the directory is created on open
    errors: {path: logs/error.log}
    elasticsearch: {disabled: true}
```

```go
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
	"github.com/linzeyan/loadconf/logger"
)

type Config struct {
	Log   config.Log
	MySQL config.MySQL
}

func main() {
	ctx := context.Background()
	cfg, err := config.Load[Config](ctx, config.From(config.Profile("app"), config.Env("APP")))
	if err != nil {
		slog.Error("load config", "error", err) // no logger yet: slog's default writes to stderr
		os.Exit(1)
	}
	// New opens every output, so a bad path fails here, not at the first
	// record. Once running, a failing output (full disk, Elasticsearch down)
	// does not affect the others and never makes a log call fail; its errors
	// go to logger.WithErrorHandler (default: stderr, at most once per output
	// every 10s).
	// stdout, stderr, file, syslog and gelf write on the calling goroutine,
	// so a slow one slows every log call. elasticsearch and otlp queue the
	// records and send them in batches; when the queue is full they drop
	// records and report how many.
	log, err := logger.New(ctx, cfg.Log)
	if err != nil {
		slog.Error("open logger", "error", err)
		os.Exit(1)
	}
	defer log.Close() // flushes the Elasticsearch queue
	// slog, the log package, and connections opened from here on write
	// through log.
	log.SetDefault()

	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Error("open mysql", "error", err)
		return
	}
	defer conn_gorm.Close(db)

	log.Debug("cache warmed", "keys", 1200)
	log.Info("started")
	log.Warn("upstream slow", "ms", 900)
	log.Error("payment failed", "order", 42)
}
```

Where each record goes (verified by running this configuration):

| Call | stdout | `app.log` | `error.log` | Elasticsearch |
|---|---|---|---|---|
| `log.Debug` | dev only | dev only | | |
| `log.Info` | ✓ | ✓ | | |
| `log.Warn` | ✓ | ✓ | | outside dev |
| `log.Error` | ✓ | ✓ | ✓ | outside dev |

```bash
APP_LOG__OUTPUTS__ELASTICSEARCH__DISABLED=true   # turns one output off at deploy time
```

At run time, `log.Update` changes the levels, global and per output, at once (example 9). Adding or removing an output, or changing a `format`, takes a new logger. The fields of syslog, gelf and otlp are under [Logger](#logger).

### 9 Hot reload and reconnecting

**Changes from example 8:** `config.Load` becomes `config.New`, and the loader watches the sources. Log levels follow a change immediately; connections are rebuilt in `OnChange`.

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"sync/atomic"
	"time"

	"gorm.io/gorm"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
	"github.com/linzeyan/loadconf/logger"
)

type Config struct {
	Log      config.Log
	MySQL    config.MySQL
	MaxItems int `default:"100"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	loader := config.New[Config](config.From(config.Profile("app"), config.Env("APP")))
	cfg, err := loader.Load(ctx)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}
	log, err := logger.New(ctx, cfg.Log)
	if err != nil {
		slog.Error("open logger", "error", err)
		os.Exit(1)
	}
	defer log.Close()
	log.SetDefault()

	first, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Error("open mysql", "error", err)
		return
	}
	// The rest of the program reads the connection through db.Load(), so a
	// swap reaches every later query.
	var db atomic.Pointer[gorm.DB]
	db.Store(first)
	defer func() { conn_gorm.Close(db.Load()) }()

	// OnChange runs when a reload passes validation and the result differs
	// from the current configuration. Callbacks run in order on the Watch
	// goroutine, and the next reload waits for them to return.
	loader.OnChange(func(old, next *Config) {
		if err := log.Update(next.Log); err != nil {
			log.Error("log config not applied", "error", err)
		}
		// Connections are never rebuilt for you. For a Named field,
		// old.MySQL.Diff(next.MySQL) returns the added, removed and changed
		// names, so only those are rebuilt.
		if reflect.DeepEqual(old.MySQL, next.MySQL) {
			return
		}
		// Open the new connection first: if the new settings are wrong, the
		// old connection keeps serving.
		fresh, err := conn_gorm.OpenMySQL(ctx, next.MySQL)
		if err != nil {
			log.Error("mysql config not applied, keeping the old connection", "error", err)
			return
		}
		stale := db.Swap(fresh)
		// A request may have loaded the old handle just before the swap; give
		// it time to start its query. database/sql's Close then waits for
		// running queries to finish.
		time.AfterFunc(time.Minute, func() { _ = conn_gorm.Close(stale) })
		log.Info("mysql reconnected", "host", next.MySQL.Host)
	})
	// OnError runs when a reload fails at any step: reading a source,
	// parsing, decoding or validating. The loader has already logged the
	// error and keeps the current configuration.
	var reloadFailures atomic.Int64 // e.g. exported as a metric for alerting
	loader.OnError(func(err error) { reloadFailures.Add(1) })
	// Watch reloads 500ms after the last change (config.WithDebounce changes
	// the delay) when:
	//   - a file of File, Profile or DotEnv changes. It watches the directory,
	//     so editors that save by rename and Kubernetes ConfigMap symlink
	//     swaps are seen;
	//   - an etcd key or prefix changes.
	// Environment variables do not change while the process runs. Watch
	// returns nil when ctx is done, and an error when no source can be
	// watched or a watch breaks down.
	go func() {
		if err := loader.Watch(ctx); err != nil {
			log.Error("config watch stopped", "error", err)
		}
	}()

	// loader.Current() returns the latest configuration, shared by every
	// goroutine: read it, never modify it.
	serve(ctx, &db, func() int { return loader.Current().MaxItems })
}

// serve stands for the application. It calls db.Load() and maxItems() once
// per request, so a request sees one version and the next sees the latest.
func serve(ctx context.Context, db *atomic.Pointer[gorm.DB], maxItems func() int) {
	<-ctx.Done()
}
```

For Redis, close the stale client in its own goroutine (`go stale.Close()`): go-redis takes about 2s to close a client that never connected.

### 10 Configuration for each layer

**Changes from example 9:** the whole `Config` is no longer passed around. Each layer declares the settings it needs, and `main` composes them.

`order/order.go`:

```go
// Package order is one layer of the application. It declares the settings
// it needs and does not know where they come from.
package order

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// Config is this layer's part of the configuration. Its defaults and rules
// apply when main loads the whole tree, and errors carry the full path
// (order.max_items: ...). For a rule across fields, add a Validate() error
// method.
type Config struct {
	MaxItems int           `default:"100" validate:"min=1"`
	Timeout  time.Duration `default:"3s"`
}

type Order struct {
	ID    uint
	Items int
}

// Repo receives a connection, not connection settings.
type Repo struct {
	db func() *gorm.DB // a func, so a reconnect (example 9) is picked up
}

func NewRepo(db func() *gorm.DB) *Repo { return &Repo{db: db} }

func (r *Repo) Create(ctx context.Context, o *Order) error {
	return r.db().WithContext(ctx).Create(o).Error
}

// Service receives its settings as a func so that a reload reaches it. A
// setting that never changes at run time can be passed as a plain value.
type Service struct {
	cfg  func() Config
	repo *Repo
}

func NewService(cfg func() Config, repo *Repo) *Service { return &Service{cfg: cfg, repo: repo} }

var ErrTooManyItems = errors.New("too many items")

func (s *Service) Place(ctx context.Context, items int) error {
	cfg := s.cfg() // once per call, so one call sees one version
	if items > cfg.MaxItems {
		return ErrTooManyItems
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	return s.repo.Create(ctx, &Order{Items: items})
}
```

`main.go`:

```go
package main

import (
	"context"
	"log"

	"gorm.io/gorm"

	"example.com/app/order"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
)

// Config only composes what the layers declare.
type Config struct {
	MySQL config.MySQL
	Order order.Config // keys order.max_items and order.timeout
}

func main() {
	ctx := context.Background()
	loader := config.New[Config](config.From(config.Profile("app"), config.Env("APP")))
	cfg, err := loader.Load(ctx)
	if err != nil {
		log.Fatal(err)
	}
	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn_gorm.Close(db)

	// Pass each layer its values or connections, never the loader or the
	// whole Config: a layer then depends only on its own types.
	repo := order.NewRepo(func() *gorm.DB { return db })
	svc := order.NewService(func() order.Config { return loader.Current().Order }, repo)
	go loader.Watch(ctx) // handle its error as in example 9

	_ = svc // hand svc to the HTTP or gRPC layer
}
```

`order/order_test.go`:

```go
// A layer's tests pass values directly: no config file, no loader.
func TestPlaceRejectsTooManyItems(t *testing.T) {
	s := NewService(func() Config { return Config{MaxItems: 2} }, nil)
	if err := s.Place(context.Background(), 3); err != ErrTooManyItems {
		t.Fatalf("err = %v", err)
	}
}
```

## Reference

### Sources and precedence

```go
cfg, err := config.Load[Config](ctx, config.From(
	config.Profile("app"),                                 // configs/app.yaml + configs/app.$ENV.yaml
	config.File("/etc/app/extra.toml", config.Optional()), // .json .yaml .yml .toml; Optional: may be missing
	config.DotEnv(".env", "APP", config.DotEnvOptional()), // same names as Env
	etcd.Prefix(cli, "/order-service/prod/"),              // /order-service/prod/mysql/main/host -> mysql.main.host
	config.Env("APP"),                                     // real environment variables
))
// A later source overrides an earlier one; maps merge key by key and Named
// instances merge by name. Fields with an env tag are set after all sources.
// Also available: config.Bytes("yaml", data) and config.Map(m), for tests and
// defaults built in code. A custom source implements config.Source, and
// config.WatchableSource to take part in hot reload.
```

### Which sources read ENV

Only `Profile` reads `ENV`. Every other source reads the path, prefix or key it is given; to vary it by environment, put the environment into that path or key.

| Source | Reads `ENV` | Varies by environment through |
|---|---|---|
| `config.Profile("app")` | yes: `app.yaml`, then `app.$ENV.yaml`; with `$CONFIG_PATH` set, that file only | file name |
| `config.File(path)` | no | the path you build |
| `config.Env("APP")`, `config.DotEnv` | no | the values each deployment sets |
| `etcd.Key`, `etcd.Prefix` | no | the key: `"/order-service/"+env+"/"` |
| `env` tag | no | the values each deployment sets |

### Environment variable names

```bash
# config.Env("APP") reads variables starting with APP_. "__" separates levels;
# a single "_" stays in the key. Change the separator with config.EnvSeparator.
APP_PORT=9090                          # port
APP_MYSQL__MAIN__PASSWORD=s3cret       # mysql.main.password
APP_MYSQL__MAIN__CONN_MAX_LIFETIME=10m # mysql.main.conn_max_lifetime
APP_REDIS__ADDRS=r1:6379,r2:6379       # a list: comma separated
APP_MYSQL__REPORT__HOST=10.0.0.3       # adds the instance "report"
APP_MYSQL__MAIN__PARAMS__PARSETIME=1   # map keys arrive lower-cased: parsetime
# An empty value counts as unset. A variable that matches no field is ignored
# without a warning, even under WithStrict: environments hold many unrelated
# variables.
```

### Struct tags

```go
type Config struct {
	Port    int             `default:"8080" env:"PORT"` // env: read $PORT after all sources
	Timeout time.Duration   `default:"5s"`              // durations are strings: "500ms", "5m"
	Buffer  config.ByteSize `default:"64MiB"`           // 64MiB = 64×1024², 64MB = 64×1000²
	Hosts   []string        `validate:"min=1"`          // go-playground/validator rules
	Token   config.Secret   // prints as ******; Token.Value() is the real string
	GRPC    GRPCConfig      `env-prefix:"GRPC_"`        // prefix for the env tags inside GRPCConfig
	Legacy  string          `config:"legacy_name"`      // explicit key; "-" skips the field
}
// A type can hook into loading by implementing SetDefaults(), Validate() error
// or UnmarshalConfig(any) error. encoding.TextUnmarshaler types, such as
// netip.Addr, decode from strings. config.WithTagName("yaml") reads yaml tags
// instead of config tags.
```

### Open and New

```go
rdb, err := conn_redis.Open(ctx, cfg.Redis) // build, then check within ping_timeout; on failure, close and return the error
rdb, err := conn_redis.New(cfg.Redis)       // build only, no network I/O; connects on first use
o, err := conn_redis.Options(cfg.Redis)     // the driver's own settings, to build a client yourself
```

A connector has no `New` when its driver cannot build a client without connecting, and no `Open` when it is an HTTP API client with no connection to check.

| Connector | Open | New | Driver settings |
|---|---|---|---|
| `conn_sql` | `OpenMySQL`, `OpenPostgres` | `NewMySQL`, `NewPostgres` | `MySQLConfig`, `PostgresConfig`, `PostgresConnString` |
| `conn_mssql` | `Open` | `New` | `Config`, `Connector` |
| `conn_oracle` | `Open` | `New` | `URL`, `Connector` |
| `conn_sqlite` | `Open` | `New` | `DSN` |
| `conn_trino` | `Open` | `New` | `DSN` |
| `conn_clickhouse` | `Open` (native API), `OpenDB` (database/sql) | `New`, `NewDB` | `Options`, `Connector` |
| `conn_gorm` | `OpenMySQL`, `OpenPostgres` | none: `gorm.Open` may connect while initializing the dialector (MySQL queries the server version) | |
| `conn_gorm_mssql`, `conn_gorm_sqlite` | `Open` | none: as `conn_gorm` | |
| `conn_redis` | `Open` | `New` | `Options` |
| `conn_mongo` | `Open`, `OpenDatabase` | `New` | `ClientOptions` |
| `conn_cassandra`, `conn_scylla` | `Open` | none: gocql connects when it creates a session | `ClusterConfig` |
| `conn_es8`, `conn_es9` | `Open` | `New`, `NewTypedClient` | `Config` |
| `conn_opensearch` | `Open` | `New` | `Config` |
| `conn_kafka_franz` | `Open` | `New` | `Options` |
| `conn_kafka_sarama` | `OpenClient`, `OpenSyncProducer`, `OpenAsyncProducer`, `OpenConsumerGroup`, `OpenConsumer`, `OpenClusterAdmin` | none: sarama connects when it builds a client | `Config` |
| `conn_mqtt` | `Open` | `New` | `ClientOptions` |
| `conn_mqtt_rest` | none: HTTP API | `New`, `NewEMQX`, `NewGeneric` | |
| `conn_streamload` | none: HTTP API | `New` | |

```go
// Every entry point takes options for what a config file cannot hold.
db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("main"),
	conn_gorm.WithGormConfig(&gorm.Config{PrepareStmt: true}))

// gocql and sarama cannot be interrupted while connecting. When ctx ends
// first, Open returns ctx.Err() at once and closes the session or client
// that arrives later. Give ctx a deadline, or bound the driver itself with
// options.connect_timeout (gocql) or options.net.dial_timeout (sarama).
ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()
session, err := conn_cassandra.Open(ctx, cfg.Cassandra)
```

### Connectors

#### SQL

```go
import (
	"github.com/linzeyan/loadconf/connector/conn_gorm"
	"github.com/linzeyan/loadconf/connector/conn_gorm_mssql"
	"github.com/linzeyan/loadconf/connector/conn_gorm_sqlite"
	"github.com/linzeyan/loadconf/connector/conn_mssql"
	"github.com/linzeyan/loadconf/connector/conn_oracle"
	"github.com/linzeyan/loadconf/connector/conn_sql"
	"github.com/linzeyan/loadconf/connector/conn_sqlite"
)

sqlDB, err := conn_sql.OpenMySQL(ctx, cfg.MySQL.MustGet("main"))       // *sql.DB
pg, err := conn_sql.OpenPostgres(ctx, cfg.Postgres.MustGet("main"))    // *sql.DB through pgx
gdb, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("main"))        // *gorm.DB; turns parseTime on unless set
gpg, err := conn_gorm.OpenPostgres(ctx, cfg.Postgres.MustGet("main"))
mss, err := conn_mssql.Open(ctx, cfg.SQLServer.MustGet("erp"))
gmss, err := conn_gorm_mssql.Open(ctx, cfg.SQLServer.MustGet("erp"))
ora, err := conn_oracle.Open(ctx, cfg.Oracle.MustGet("core"))          // pure Go: no Instant Client
lite, err := conn_sqlite.Open(ctx, cfg.SQLite.MustGet("local"))        // no cgo
glite, err := conn_gorm_sqlite.Open(ctx, cfg.SQLite.MustGet("local"))
```

```yaml
# config.MySQL also serves TiDB, OceanBase, and Doris and StarRocks (the FE's
# port 9030). config.Postgres also serves CockroachDB, YugabyteDB, Redshift,
# TimescaleDB and Greenplum.
mysql:
  main:
    dsn: "app@tcp(db1:3306)/shop"   # or host, port, user, database
    password: s3cret                # fields override the DSN, e.g. a password from a secret store
    params: {loc: Local}
    max_open_conns: 20
sqlserver:
  erp: {host: mssql, user: sa, password: pw, database: erp, params: {encrypt: "true"}}
oracle:
  core: {host: ora, service: FREEPDB1, user: app, password: pw}
sqlite:
  local: {path: data/app.db}        # or ":memory:"
```

#### NoSQL

```go
import (
	"github.com/linzeyan/loadconf/connector/conn_cassandra"
	"github.com/linzeyan/loadconf/connector/conn_mongo"
	"github.com/linzeyan/loadconf/connector/conn_redis"
	"github.com/linzeyan/loadconf/connector/conn_scylla"
)

rdb, err := conn_redis.Open(ctx, cfg.Redis)          // redis.UniversalClient: single, cluster or sentinel
mdb, err := conn_mongo.OpenDatabase(ctx, cfg.Mongo)  // *mongo.Database; conn_mongo.Open returns the client
cass, err := conn_cassandra.Open(ctx, cfg.Cassandra) // *gocql.Session
scy, err := conn_scylla.Open(ctx, cfg.Scylla)        // shard-aware with the replace directive below
```

```yaml
redis:
  addrs: ["r1:6379", "r2:6379", "r3:6379"]  # several addresses: cluster; with master_name: sentinel
  password: s3cret
mongo:
  uri: "mongodb://m1,m2/?replicaSet=rs0"
  database: shop
scylla:
  hosts: [s1, s2]
  local_dc: dc1
  options: {consistency: LOCAL_QUORUM}      # gocql expects capitals
```

```
// go.mod of an application that uses conn_scylla. Without this line the
// build succeeds and connects through upstream gocql: no shard awareness,
// and Open logs a warning.
replace github.com/gocql/gocql => github.com/scylladb/gocql v1.19.0
```

#### OLAP

```go
import (
	"github.com/linzeyan/loadconf/connector/conn_clickhouse"
	"github.com/linzeyan/loadconf/connector/conn_streamload"
	"github.com/linzeyan/loadconf/connector/conn_trino"
)

ch, err := conn_clickhouse.Open(ctx, cfg.ClickHouse)     // native API: fastest for batch inserts
chDB, err := conn_clickhouse.OpenDB(ctx, cfg.ClickHouse) // database/sql
tr, err := conn_trino.Open(ctx, cfg.Trino)               // checked with SELECT 1

// Bulk loads into Doris or StarRocks: Stream Load rather than INSERT.
// A failed request is retried on the next FE under the same label, and the
// server deduplicates by label, so a batch is loaded at most once. Data
// errors are not retried: they return a *conn_streamload.LoadError with
// ErrorURL.
sl, err := conn_streamload.New(cfg.Doris) // cfg.Doris is a config.StreamLoad

res, err := sl.LoadJSON(ctx, "events", rows) // rows: a slice, sent as one JSON array
res, err = sl.LoadCSV(ctx, "events", records, conn_streamload.LoadOptions{
	Headers: map[string]string{"columns": "id,name"},
})

w, err := sl.NewWriter("events") // flushes at max_rows, max_bytes or flush_interval
err = w.WriteJSON(ctx, event)
err = w.Close(ctx)

// Two-phase commit. Doris accepts one Load per transaction, StarRocks
// several. A retry resends the data, so pass a seekable reader
// (*bytes.Reader, *os.File).
tx, err := sl.Begin(ctx, "orders")
if _, err := tx.Load(ctx, bytes.NewReader(data)); err != nil {
	_ = tx.Abort(ctx)
} else {
	err = tx.Commit(ctx)
}
```

```yaml
doris:
  flavor: doris             # or starrocks
  addrs: ["fe1:8030", "fe2:8030"]
  database: dw
  password: pw
  headers: {strict_mode: "true"}
```

#### Search

```go
import (
	"github.com/linzeyan/loadconf/connector/conn_es8" // or conn_es9
	"github.com/linzeyan/loadconf/connector/conn_opensearch"
)

es, err := conn_es8.Open(ctx, cfg.Elasticsearch)
typed, err := conn_es8.NewTypedClient(cfg.Elasticsearch)
osc, err := conn_opensearch.Open(ctx, cfg.OpenSearch)
```

```yaml
elasticsearch:
  addresses: ["https://es1:9200"]
  api_key: <key>            # or username and password, service_token, cloud_id
  options: {max_retries: 5, compress_request_body: true}
```

#### Messaging

```go
import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/linzeyan/loadconf/connector/conn_kafka_franz"
	"github.com/linzeyan/loadconf/connector/conn_kafka_sarama"
	"github.com/linzeyan/loadconf/connector/conn_mqtt"
	"github.com/linzeyan/loadconf/connector/conn_mqtt_rest"
)

// franz-go: one client produces and consumes; its settings are kgo options.
kc, err := conn_kafka_franz.Open(ctx, cfg.Kafka,
	kgo.DefaultProduceTopic("events"), kgo.ProducerBatchCompression(kgo.ZstdCompression()))

// sarama: settings go in options. The producer is idempotent and waits for
// all in-sync replicas by default.
prod, err := conn_kafka_sarama.OpenSyncProducer(ctx, cfg.Kafka)
group, err := conn_kafka_sarama.OpenConsumerGroup(ctx, cfg.Kafka)
err = group.Consume(ctx, cfg.Kafka.Consumer.Topics, handler)

// MQTT 3.1.1 through Paho. With options.connect_retry set, Open keeps
// retrying: give ctx a deadline.
mc, err := conn_mqtt.Open(ctx, cfg.MQTT)

// EMQX REST API, versions 5.8 to 6.3.
emqx, err := conn_mqtt_rest.NewEMQX(cfg.EMQX)
res, err := emqx.Publish(ctx, conn_mqtt_rest.Message{Topic: "a/b", Payload: []byte("hi"), QoS: 1})
// res.NoSubscribers: published, but nobody was subscribed
```

```yaml
kafka:
  brokers: ["k1:9092"]
  consumer: {group: order-service, topics: [orders], initial_offset: earliest}
mqtt:
  brokers: ["tcp://emqx:1883"]
  client_id: order-service
  client_id_random_suffix: true   # replicas of one service must not share a client ID, or they disconnect each other
emqx:
  base_url: http://emqx:18083
  auth: {username: <api-key>, password: <api-secret>}
```

### Logger

```go
import (
	"github.com/linzeyan/loadconf/logger"
	"github.com/linzeyan/loadconf/logger/logger_zap"
	"github.com/linzeyan/loadconf/logger/logger_zerolog"
)

log, err := logger.New(ctx, cfg.Log) // embeds *slog.Logger
defer log.Close()                    // flushes the queues of the elasticsearch and otlp outputs
log.SetDefault()
// Connections opened after SetDefault log through log:
//   gorm: failed queries, and queries slower than 200ms
//   MySQL, PostgreSQL: warnings and above
//   MongoDB, Cassandra/Scylla, ClickHouse, Kafka (franz-go), Stream Load
//   Elasticsearch, OpenSearch: every request; 4xx and 5xx as warnings

zl, err := logger_zap.New(ctx, cfg.Log)     // the same configuration and outputs, as zap
zr, err := logger_zerolog.New(ctx, cfg.Log) // or as zerolog
```

```yaml
log:
  level: info                 # trace, debug, info, warn, error; hot reload applies changes
  format: json                # or text: stdout, stderr and file only
  add_source: true
  service: order-service      # adds a service field
  fields: {env: prod}         # static fields on every record
  outputs:                    # stdout when empty
    stdout: {}
    file: {path: /var/log/app/app.log, max_size: 100MiB, max_backups: 7, max_age: 72h, compress: true}
    errors: {type: file, path: /var/log/app/error.log, level: error}
    gelf: {addr: "graylog:12201"}
    syslog: {network: udp, addr: "10.0.0.5:514"}
    elasticsearch: {addresses: ["https://es:9200"], index: "logs-{service}-{date:2006.01.02}"}
```

```go
import (
	"github.com/linzeyan/loadconf/connector/conn_gorm_log"
	"github.com/linzeyan/loadconf/logger/sink_otlp"
)

// OTLP is a separate module because of its gRPC dependencies; pass it in.
log, err := logger.New(ctx, cfg.Log,
	logger.WithSink("otlp", sink_otlp.Open),
	logger.WithContextAttrs(sink_otlp.TraceAttrs)) // adds trace_id and span_id from ctx
log.InfoContext(ctx, "handled")

// A driver logger other than the default is passed with WithLogger. gorm's
// levels can live in the config file:
type Config struct {
	GormLog conn_gorm_log.Config // slow_threshold, level, parameterized_queries
}
gdb, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("main"),
	conn_gorm.WithGormConfig(&gorm.Config{Logger: conn_gorm_log.New(nil, cfg.GormLog)}))
es, err := conn_es8.Open(ctx, cfg.Elasticsearch, conn_es8.WithLogger(auditLog))

// These drivers only have a process-wide logger; set it once.
redis.SetLogger(conn_redis.SlogLogger(log.Logger, slog.LevelWarn))
sarama.Logger = conn_kafka_sarama.SlogLogger(log.Logger, slog.LevelInfo)
mssql.SetContextLogger(conn_mssql.SlogLogger(log.Logger))
restore := conn_mqtt.SetLogger(log.Logger)
```

## Versioning

```bash
# Every module is released under the same version; upgrade them together.
go get github.com/linzeyan/loadconf/config@v0.3.0 github.com/linzeyan/loadconf/connector/conn_gorm@v0.3.0
# Mixed versions compile, but Go selects the highest version any module
# requires of a shared module such as config, so an older pin does not hold.
```

Development, testing and releases: [CONTRIBUTING.md](CONTRIBUTING.md).
