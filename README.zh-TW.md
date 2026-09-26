# loadconf

[English](README.md) | 繁體中文

宣告一個 struct、寫一份設定檔，就能拿到驗證過的設定、logger，以及各 driver 原生的 client（`*sql.DB`、`*gorm.DB`、`redis.UniversalClient`、`*kgo.Client` 等）。

每個 connector 都是獨立的 Go module，只用 Redis 的服務不會依賴 Kafka 或 Oracle 的 driver。需要 Go 1.26 以上。

## 目錄

- [安裝](#安裝)
- [範例](#範例)
  1. [一個設定檔](#1-一個設定檔)
  2. [依環境分檔](#2-依環境分檔)
  3. [環境變數與密碼](#3-環境變數與密碼)
  4. [多個資料庫](#4-多個資料庫)
  5. [設定放在 etcd](#5-設定放在-etcd)
  6. [driver 參數](#6-driver-參數)
  7. [讀寫分離](#7-讀寫分離)
  8. [Log 輸出到多個地方](#8-log-輸出到多個地方)
  9. [熱更新與重新連線](#9-熱更新與重新連線)
  10. [把設定注入各層](#10-把設定注入各層)
- [參考](#參考)
  - [來源與優先順序](#來源與優先順序)
  - [哪些來源會看 ENV](#哪些來源會看-env)
  - [環境變數的名稱](#環境變數的名稱)
  - [Struct tag](#struct-tag)
  - [Open 與 New](#open-與-new)
  - [Connector 一覽](#connector-一覽)
  - [Logger](#logger)
- [版本](#版本)

## 安裝

```bash
# 所有 module 用同一個版本號一起發佈：每個 module 都指定同一個版本。
go get github.com/linzeyan/loadconf/config@v0.1.0 \
  github.com/linzeyan/loadconf/logger@v0.1.0 \
  github.com/linzeyan/loadconf/connector/conn_gorm@v0.1.0
```

import 路徑就是 module 路徑，package 名稱是路徑的最後一段。

| import 路徑 | 內容 |
|---|---|
| `github.com/linzeyan/loadconf/config` | 載入設定，以及所有連線的設定型別（`config.MySQL`、`config.Redis` 等） |
| `github.com/linzeyan/loadconf/config/etcd` | etcd 來源。獨立的 module，不用 etcd 的服務不會依賴 etcd client |
| `github.com/linzeyan/loadconf/logger` | slog logger 與各種輸出 |
| `github.com/linzeyan/loadconf/logger/logger_zap`、`…/logger_zerolog`、`…/sink_otlp` | 用同一份設定建立 zap 或 zerolog logger；OTLP 輸出 |
| `github.com/linzeyan/loadconf/connector/conn_<driver>` | 每個 driver 一個 module，見 [Connector 一覽](#connector-一覽) |

## 範例

每個範例都在前面某個範例上多加一件事，第一行寫明加了什麼。

### 1 一個設定檔

`config.yaml`：

```yaml
# key 是欄位名稱的 snake_case，產品名稱保持完整：
# MySQL -> mysql、MaxOpenConns -> max_open_conns。比對不分大小寫。
port: 8080
mysql:
  host: 127.0.0.1         # 沒寫 port 就是 3306
  user: app
  password: dev-only
  database: shop
  max_open_conns: 20      # 沒寫或寫 0：沿用 database/sql 的預設，不限制
  conn_max_lifetime: 5m
```

`main.go`：

```go
package main

import (
	"context"
	"log"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
)

// 欄位不需要 tag。
type Config struct {
	Port  int `default:"8080"` // 沒有任何來源設定 port 時使用
	MySQL config.MySQL
}

func main() {
	ctx := context.Background()
	// 副檔名決定格式：.yaml、.yml、.json 或 .toml。
	// Load 依序合併來源、套用預設值、解碼、驗證，並一次列出所有問題，
	// 每一筆都帶 key 的完整路徑：
	//   config: decode: mysql.max_open_conns: invalid integer "many"
	//   mysql.port: invalid integer "abc"
	// 對不到任何欄位的 key（通常是拼錯字）只會記一筆警告：
	//   WARN config keys match no field and are ignored source=file(config.yaml) keys=[mysql.hots]
	// 加上 config.WithStrict() 就改為錯誤。
	cfg, err := config.Load[Config](ctx, config.From(config.File("config.yaml")))
	if err != nil {
		log.Fatal(err)
	}

	// OpenMySQL 建立連線池，並在 ping_timeout（預設 3s）內 ping。
	// ping 失敗時，先關掉連線池再回傳錯誤。
	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn_gorm.Close(db)

	log.Printf("listening on :%d", cfg.Port)
}
```

### 2 依環境分檔

**和範例 1 的差別**：`config.File("config.yaml")` 換成 `config.Profile("app")`。

```
configs/
├── app.yaml        # 所有環境共用；可以沒有
├── app.dev.yaml    # ENV=dev，以及沒設 ENV 時
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
# configs/app.dev.yaml：逐個 key 合併進 app.yaml
mysql:
  host: 127.0.0.1
  password: dev-only
```

```yaml
# configs/app.prod.yaml：密碼改由環境變數提供（範例 3）
mysql:
  host: db.prod.internal
  max_open_conns: 100     # 蓋掉 app.yaml 的 20；user、database 保留
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
	// 先讀 configs/app.yaml，再用 configs/app.$ENV.yaml 蓋上去。
	// 每次載入都會記下實際讀了哪些檔案：
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

| 執行時 | 讀的檔案 | `mysql.host` | `mysql.max_open_conns` |
|---|---|---|---|
| 沒設 `ENV`，或 `ENV=dev` | `app.yaml`，再讀 `app.dev.yaml` | `127.0.0.1` | 20 |
| `ENV=prod` | `app.yaml`，再讀 `app.prod.yaml` | `db.prod.internal` | 100 |
| `ENV=stage` | 不讀任何檔案，`Load` 失敗：`no config file app.stage{.yaml,.yml,.json,.toml} in ./configs` | | |
| `CONFIG_PATH=/etc/app/app.yaml` | 只讀這個檔，不看 `ENV` | 取自該檔 | 取自該檔 |

`Profile` 的選項：

```go
config.Profile("app", config.ProfileDir("/etc/app"))              // 檔案所在目錄，預設 ./configs
config.Profile("app", config.ProfileEnvs("dev", "stage", "prod")) // 其他 ENV 一律失敗：unsupported ENV="prdo" (want one of dev, stage, prod)
config.Profile("app", config.ProfileEnvVar("APP_ENV"))            // 改讀 APP_ENV，不讀 ENV
config.Profile("app", config.ProfileDefaultEnv("local"))          // 變數沒設時的環境，預設 dev
config.Profile("app", config.ProfilePathEnvVar("APP_CONFIG"))     // 指定單一檔案的變數，預設 CONFIG_PATH
```

### 3 環境變數與密碼

**和範例 2 的差別**：來源多了 `config.DotEnv` 和 `config.Env`；`PaymentKey` 的型別是 `config.Secret`。

```go
package main

import (
	"context"
	"log"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm"
)

type Config struct {
	// env:"PORT" 在所有來源之後讀取 $PORT，
	// 所以平台指定的 port 會蓋過設定檔，也蓋過 APP_PORT。
	Port int `default:"8080" env:"PORT"`
	// config.MySQL 的密碼欄位本來就是 config.Secret。
	MySQL config.MySQL
	// 非空的 Secret 用任何 fmt 格式印出都是 ******；Value() 才會回傳原文。
	PaymentKey  config.Secret
	PaymentHost string `default:"https://pay.example.com"`
}

func main() {
	ctx := context.Background()
	// 後面的來源逐個 key 蓋過前面的。
	cfg, err := config.Load[Config](ctx, config.From(
		config.Profile("app"),
		// 工作目錄下的 .env，給本機開發用。沒加 DotEnvOptional 時，
		// 檔案不存在就是錯誤。名稱對應 key 的規則和下面的 Env 相同。
		config.DotEnv(".env", "APP", config.DotEnvOptional()),
		// APP_MYSQL__PASSWORD 設定 mysql.password：「__」分隔層級，
		// 單一個「_」留在 key 裡。空值視為沒設定。
		config.Env("APP"),
	))
	if err != nil {
		log.Fatal(err)
	}
	// 印出 ... Password:****** ... PaymentKey:******
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
# .env：只給本機開發用，不要進版控
APP_MYSQL__PASSWORD=dev-only
APP_PAYMENT_KEY=test-key
```

```bash
# 正式環境
ENV=prod                      # 選用 configs/app.prod.yaml
APP_MYSQL__PASSWORD=s3cret    # mysql.password
APP_PAYMENT_KEY=live-key      # payment_key
PORT=9090                     # Port 的 env tag；APP_PORT=9090 也可以，但兩個都設時 PORT 優先
```

### 4 多個資料庫

**和範例 3 的差別**：`config.MySQL` 換成 `config.Named[config.MySQL]`，程式用名稱挑實例。

```yaml
# configs/app.yaml
mysql:                   # map：每個 key 是實例名稱
  orders:
    host: db1
    user: app
    database: orders
  report:
    host: db2
    user: report
    database: report
redis:                   # 也可以寫成清單，每一項帶 name
  - name: cache
    addrs: ["r1:6379"]
  - name: session
    addrs: ["r2:6379", "r3:6379"]   # 超過一個位址：cluster client
# 在 flow mapping {...} 裡，host:port 要加引號：{addrs: [r1:6379]} 會把
# r1:6379 解析成 map。上面這種 block 寫法不需要引號。
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
	// Named 依名稱存放多個實例。只有一個資料庫時，
	// 用 config.MySQL 就夠了（範例 1～3）。
	MySQL config.Named[config.MySQL]
	Redis config.Named[config.Redis]
}

func main() {
	ctx := context.Background()
	cfg, err := config.Load[Config](ctx, config.From(config.Profile("app"), config.Env("APP")))
	if err != nil {
		log.Fatal(err)
	}

	// 名稱不存在時 MustGet 會 panic，適合在啟動階段使用。
	// 名稱比對不分大小寫。
	orders, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("orders"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn_gorm.Close(orders)

	// Get 會回報實例是否存在，用在可有可無的實例。
	if reportCfg, ok := cfg.MySQL.Get("report"); ok {
		report, err := conn_gorm.OpenMySQL(ctx, reportCfg)
		if err != nil {
			log.Fatal(err)
		}
		defer conn_gorm.Close(report)
	}

	// All 依名稱排序，逐一列出所有實例。
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

多個來源依實例名稱逐一合併，所以環境變數可以只改某個實例的某個欄位，也可以新增實例：

```bash
APP_MYSQL__REPORT__PASSWORD=s3cret   # report 的其他欄位不變
APP_MYSQL__ARCHIVE__HOST=10.0.0.3    # 新增實例 archive，完全由環境變數定義
```

### 5 設定放在 etcd

**和範例 4 的差別**：設定改從 etcd 讀。程式得先知道 etcd 在哪裡才讀得到，所以分兩段載入。

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

// Bootstrap 是連上 etcd 之前需要的設定。
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
	// ENV 只決定 Profile 讀哪個檔名。etcd 的 key 寫什麼就讀什麼，
	// 所以環境必須放進 key 裡。cmp.Or 套用和 Profile 相同的規則
	// （沒設就是 dev），讓檔案和 etcd 的環境一致。
	env := cmp.Or(os.Getenv("ENV"), "dev")

	// 第一段，從環境變數讀：APP_ETCD__ENDPOINTS=etcd1:2379,etcd2:2379
	boot, err := config.Load[Bootstrap](ctx, config.From(config.Env("APP")))
	if err != nil {
		log.Fatal(err)
	}
	// 有 Username 時，clientv3.New 會立刻認證，etcd 連不上就失敗；
	// 沒有時，New 不做任何 I/O，要到第一次讀取才會失敗。
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

	// 第二段。每次讀 etcd 最多等 5s（用 etcd.Timeout 調整）：
	//   config: load etcd-key(/order-service/prod/config.yaml): context deadline exceeded
	// key 不存在也會失敗，除非來源加上 etcd.Optional()：
	//   config: load etcd-key(/order-service/prod/config.yaml): key /order-service/prod/config.yaml not found
	cfg, err := config.Load[Config](ctx, config.From(
		// 一個 key 存整份文件，副檔名 .yaml 決定格式。
		etcd.Key(cli, "/order-service/"+env+"/config.yaml"),
		config.Env("APP"), // 仍然可以覆寫單一欄位
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

key 有三種排法：

| 來源 | etcd 裡存的內容 | 適合 |
|---|---|---|
| `etcd.Key(cli, "/order-service/prod/config.yaml")` | 一個 key 存整份 YAML | 整份設定一起審、一起改 |
| `etcd.Key(cli, "/sport/prod", etcd.Format("json"))` | 一個沒有副檔名的 key 存整份 JSON | 沿用既有的 `/{app}/{env}` JSON；`"MySQL"` 這類大寫 key 也對得上 |
| `etcd.Prefix(cli, "/order-service/prod/")` | 一個欄位一個 key，如下 | 不同欄位由不同的人或工具維護 |

```
# etcd.Prefix：前綴之後的部分以「/」分層，就是 key 的路徑。
# 值一律是字串，轉型規則和環境變數相同。
/order-service/prod/port                    9090
/order-service/prod/mysql/orders/host       db1
/order-service/prod/mysql/orders/password   s3cret
/order-service/prod/redis/cache/addrs       r1:6379,r2:6379
```

```go
// 像 app.yaml 和 app.$ENV.yaml 一樣分成共用層與環境層：
cfg, err := config.Load[Config](ctx, config.From(
	etcd.Prefix(cli, "/order-service/common/"),  // 所有環境共用
	etcd.Prefix(cli, "/order-service/"+env+"/"), // 這個環境，蓋過共用層
	config.Env("APP"),
))
```

兩種來源都能監看：在 `Loader.Watch`（範例 9）底下，key 或前綴底下有變動就會重新載入。監看從上次讀取的 revision 接著開始，中間的變動不會漏掉。

### 6 driver 參數

**和範例 4 的差別**：設定型別以外的 driver 參數寫在設定檔的 `options` 或 `params`；只有函式和 hook 寫在程式裡。

```yaml
# configs/app.yaml
redis:
  addrs: ["r1:6379"]
  # options 解碼到 driver 原生的 struct（這裡是 redis.UniversalOptions），
  # 欄位名稱用 snake_case。key 拼錯、型別不對，或這個 key 已有專屬欄位，
  # Open 都會失敗：
  #   unknown options: pool_sise
  #   options.password: set by its own config key, not in options
  # key 跟著 driver 版本走：升級後若欄位改名，Open 會用同樣的錯誤失敗，
  # 而不是默默忽略這個設定。
  options: {pool_size: 50, read_timeout: 2s}

kafka:
  brokers: ["k1:9092"]
  # 只有 conn_kafka_sarama 會讀：對應 sarama.Config，巢狀 struct 寫成巢狀 map。
  # conn_kafka_franz 遇到 options 會直接失敗；它的設定要在程式裡傳 kgo option。
  options:
    producer: {compression: zstd, retry: {max: 5}}

postgres:
  main:
    host: pg
    # params 原樣附加到 DSN（MySQL、PostgreSQL、SQL Server、Oracle、Trino、
    # MongoDB）。從環境變數進來的 key 會變成小寫，所以大小寫有意義的參數
    # （例如 MySQL 的 parseTime）請寫在檔案裡。
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
	Postgres config.Named[config.Postgres] // 開法同範例 4
}

func main() {
	ctx := context.Background()
	cfg, err := config.Load[Config](ctx, config.From(config.Profile("app"), config.Env("APP")))
	if err != nil {
		log.Fatal(err)
	}

	// 函式寫不進設定檔，所以在程式裡傳。程式傳入的 option
	// 在設定檔之後套用，會蓋過設定檔的值。
	rdb, err := conn_redis.Open(ctx, cfg.Redis, func(o *redis.UniversalOptions) {
		o.OnConnect = func(ctx context.Context, cn *redis.Conn) error { return nil }
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

	// franz-go 以函式設定，所以它的設定都是 kgo option。
	kc, err := conn_kafka_franz.Open(ctx, cfg.Kafka, kgo.DefaultProduceTopic("events"))
	if err != nil {
		log.Fatal(err)
	}
	defer kc.Close()
}
```

```bash
go doc github.com/linzeyan/loadconf/config.Redis   # 列出設定型別的所有欄位
```

### 7 讀寫分離

**和範例 4 的差別**：多一個實例當讀取用的 replica，用 `conn_gorm.WithMySQLReplicas` 掛上（PostgreSQL 用 `WithPostgresReplicas`）。

```yaml
mysql:
  orders: &orders           # &orders 替這個 mapping 命名
    host: db-primary
    user: app
    database: shop
    max_open_conns: 50
  orders_replica:
    <<: *orders             # 複製 orders 的所有 key，
    host: db-replica        # 再蓋掉這兩個
    max_open_conns: 100
```

```bash
# 上面的合併只發生在檔案裡。環境變數依實例名稱覆寫，
# 所以每個實例都要各自給密碼。
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

	// 每台 replica 的開法都和主庫相同：各自的連線池設定、開啟 parseTime，
	// 也套用 WithSQLOptions 和 WithMySQLDialector。任何一台開不起來，
	// OpenMySQL 就關掉已經開好的連線，並回傳錯誤。
	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("orders"),
		conn_gorm.WithMySQLReplicas(cfg.MySQL.MustGet("orders_replica")))
	if err != nil {
		log.Fatal(err)
	}
	// Close 也會關掉 replica 的連線池；db.DB() 只拿得到主庫的。
	defer conn_gorm.Close(db)

	order := Order{Status: "new"}
	db.Create(&order)
	db.Find(&[]Order{})
	// replica 可能還沒有這筆資料：必須讀到剛寫入內容的查詢，要指定主庫。
	db.Clauses(dbresolver.Write).First(&order, order.ID)
	db.Transaction(func(tx *gorm.DB) error {
		return tx.Model(&order).Update("status", "paid").Error
	})
}
```

| 操作 | 在哪裡執行 |
|---|---|
| `Create`、`Save`、`Update`、`Delete`、`Exec` | 主庫 |
| `Find`、`First`、`Count`、`Scan` | 隨機一台 replica |
| `Raw("SELECT …")` | replica；語句以 `FOR UPDATE` 結尾時改走主庫 |
| `Transaction` 裡的所有查詢，包含讀取 | 主庫 |
| 加了 `Clauses(dbresolver.Write)` 的讀取 | 主庫 |

### 8 Log 輸出到多個地方

**和範例 3 的差別**：多了 `config.Log` 和 `logger.New`。每呼叫一次 log，紀錄就會寫到所有等級符合的輸出。

```yaml
# configs/app.yaml（mysql 同範例 2）
log:
  level: info               # 所有輸出的最低等級
  service: order-service    # 每筆紀錄都加上 service=order-service
  # stack_level: error      # 預設值：error 以上的紀錄附帶 stack 欄位
  outputs:
    stdout:                 # 沒寫 type 時，輸出的名稱就是它的型別
      format: text          # stdout、stderr、file 可以用 text，其他輸出一律 JSON
    file:
      path: /var/log/order-service/app.log
      max_size: 100MiB      # 超過就輪替（預設 100MiB）
      max_backups: 7
      compress: true
    errors:                 # 第二個檔案輸出，所以要明確寫 type
      type: file
      path: /var/log/order-service/error.log
      level: error          # 這個輸出的最低等級；實際取它和 log.level 中較高的
    elasticsearch:
      addresses: ["https://es:9200"]
      index: "logs-{service}-{date:2006.01.02}"
      level: warn
```

```yaml
# configs/app.dev.yaml：輸出依名稱合併，只寫不同的部分
log:
  level: debug
  outputs:
    file: {path: logs/app.log}         # 開檔時自動建立目錄
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
		slog.Error("load config", "error", err) // 還沒有 logger：slog 預設寫到 stderr
		os.Exit(1)
	}
	// New 會開啟所有輸出，路徑有錯就在這裡失敗，不會等到第一筆紀錄。
	// 執行中某個輸出失敗（磁碟滿、Elasticsearch 掛了）不影響其他輸出，
	// 也不會讓 log 呼叫出錯；錯誤交給 logger.WithErrorHandler
	// （預設寫到 stderr，每個輸出每 10 秒最多一次）。
	// stdout、stderr、file、syslog、gelf 在呼叫端的 goroutine 裡寫入，
	// 其中一個變慢，每次 log 呼叫都跟著變慢。elasticsearch、otlp 先放進
	// 佇列再批次送出；佇列滿了就丟棄紀錄，並回報丟了幾筆。
	log, err := logger.New(ctx, cfg.Log)
	if err != nil {
		slog.Error("open logger", "error", err)
		os.Exit(1)
	}
	defer log.Close() // 送出 Elasticsearch 佇列裡剩下的紀錄
	// slog、log package，以及之後開的連線，都會寫到 log。
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

每筆紀錄會寫到哪裡（已用這份設定實際執行驗證）：

| 呼叫 | stdout | `app.log` | `error.log` | Elasticsearch |
|---|---|---|---|---|
| `log.Debug` | 只有 dev | 只有 dev | | |
| `log.Info` | ✓ | ✓ | | |
| `log.Warn` | ✓ | ✓ | | dev 以外 |
| `log.Error` | ✓ | ✓ | ✓ | dev 以外 |

```bash
APP_LOG__OUTPUTS__ELASTICSEARCH__DISABLED=true   # 部署時關掉單一輸出
```

執行中可以用 `log.Update` 立即改變等級，包括全域等級和單一輸出的等級（範例 9）。新增或移除輸出、改 `format`，則要重建 logger。syslog、gelf、otlp 的欄位見[參考：Logger](#logger)。

### 9 熱更新與重新連線

**和範例 8 的差別**：`config.Load` 換成 `config.New`，由 loader 監看來源。log 等級改了立即生效；連線要在 `OnChange` 裡自己重建。

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
	// 程式其他地方都透過 db.Load() 取得連線，
	// 所以替換之後，接下來的每個查詢都會用到新連線。
	var db atomic.Pointer[gorm.DB]
	db.Store(first)
	defer func() { conn_gorm.Close(db.Load()) }()

	// 重新載入通過驗證、而且結果和目前的設定不同時，才會呼叫 OnChange。
	// callback 在 Watch 的 goroutine 上依序執行，下一次重新載入會等它們返回。
	loader.OnChange(func(old, next *Config) {
		if err := log.Update(next.Log); err != nil {
			log.Error("log config not applied", "error", err)
		}
		// 連線不會自動重建。Named 欄位可以用 old.MySQL.Diff(next.MySQL)
		// 取得新增、移除、變更的名稱，只重建那幾個。
		if reflect.DeepEqual(old.MySQL, next.MySQL) {
			return
		}
		// 先開新連線：新設定有錯時，舊連線繼續服務。
		fresh, err := conn_gorm.OpenMySQL(ctx, next.MySQL)
		if err != nil {
			log.Error("mysql config not applied, keeping the old connection", "error", err)
			return
		}
		stale := db.Swap(fresh)
		// 某個 request 可能在替換前一刻拿到舊連線，給它時間發出查詢。
		// database/sql 的 Close 會等執行中的查詢結束。
		time.AfterFunc(time.Minute, func() { _ = conn_gorm.Close(stale) })
		log.Info("mysql reconnected", "host", next.MySQL.Host)
	})
	// 重新載入在任何一步失敗時（讀取來源、解析、解碼、驗證）呼叫 OnError。
	// loader 已經記過這筆錯誤，並繼續使用目前的設定。
	var reloadFailures atomic.Int64 // 例如輸出成 metric 供告警使用
	loader.OnError(func(err error) { reloadFailures.Add(1) })
	// 最後一次變動後等 500ms 就重新載入（用 config.WithDebounce 調整），
	// 觸發條件：
	//   - File、Profile、DotEnv 的檔案內容改變。監看的是所在目錄，所以
	//     以 rename 存檔的編輯器、Kubernetes ConfigMap 的 symlink 切換都偵測得到；
	//   - etcd 的 key 或前綴底下有變動。
	// 環境變數在程式執行中不會改變。ctx 結束時 Watch 回傳 nil；
	// 沒有可監看的來源，或監看中斷時，回傳錯誤。
	go func() {
		if err := loader.Watch(ctx); err != nil {
			log.Error("config watch stopped", "error", err)
		}
	}()

	// loader.Current() 回傳最新的設定，所有 goroutine 共用同一份：只能讀，不能改。
	serve(ctx, &db, func() int { return loader.Current().MaxItems })
}

// serve 代表應用程式本身。每個 request 各呼叫一次 db.Load() 和 maxItems()，
// 所以同一個 request 只看到一個版本，下一個 request 看到最新版本。
func serve(ctx context.Context, db *atomic.Pointer[gorm.DB], maxItems func() int) {
	<-ctx.Done()
}
```

Redis 的舊 client 請在獨立的 goroutine 裡關閉（`go stale.Close()`）：go-redis 關閉一個從沒連上的 client 大約要 2 秒。

### 10 把設定注入各層

**和範例 9 的差別**：不再把整份 `Config` 傳來傳去。每一層宣告自己需要的設定，由 `main` 組起來。

`order/order.go`：

```go
// Package order 是應用程式的其中一層。它宣告自己需要的設定，
// 但不知道設定從哪裡來。
package order

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// Config 是這一層的設定。main 載入整棵設定時，這裡的預設值和規則
// 一併生效，錯誤會帶完整路徑（order.max_items: ...）。
// 跨欄位的規則就加一個 Validate() error 方法。
type Config struct {
	MaxItems int           `default:"100" validate:"min=1"`
	Timeout  time.Duration `default:"3s"`
}

type Order struct {
	ID    uint
	Items int
}

// Repo 拿到的是連線，不是連線設定。
type Repo struct {
	db func() *gorm.DB // 用函式，才拿得到重新連線（範例 9）之後的新連線
}

func NewRepo(db func() *gorm.DB) *Repo { return &Repo{db: db} }

func (r *Repo) Create(ctx context.Context, o *Order) error {
	return r.db().WithContext(ctx).Create(o).Error
}

// Service 以函式取得設定，重新載入後才看得到新值。
// 執行中不會變的設定，直接傳值即可。
type Service struct {
	cfg  func() Config
	repo *Repo
}

func NewService(cfg func() Config, repo *Repo) *Service { return &Service{cfg: cfg, repo: repo} }

var ErrTooManyItems = errors.New("too many items")

func (s *Service) Place(ctx context.Context, items int) error {
	cfg := s.cfg() // 每次呼叫只讀一次，同一次呼叫只看到一個版本
	if items > cfg.MaxItems {
		return ErrTooManyItems
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	return s.repo.Create(ctx, &Order{Items: items})
}
```

`main.go`：

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

// Config 只負責組合各層宣告的設定。
type Config struct {
	MySQL config.MySQL
	Order order.Config // key 為 order.max_items、order.timeout
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

	// 每一層只拿到自己的值或連線，不傳 loader，也不傳整份 Config：
	// 這樣每一層只依賴自己的型別。
	repo := order.NewRepo(func() *gorm.DB { return db })
	svc := order.NewService(func() order.Config { return loader.Current().Order }, repo)
	go loader.Watch(ctx) // 錯誤處理同範例 9

	_ = svc // 交給 HTTP 或 gRPC 那一層
}
```

`order/order_test.go`：

```go
// 測試單一層時直接給值：不需要設定檔，也不需要 loader。
func TestPlaceRejectsTooManyItems(t *testing.T) {
	s := NewService(func() Config { return Config{MaxItems: 2} }, nil)
	if err := s.Place(context.Background(), 3); err != ErrTooManyItems {
		t.Fatalf("err = %v", err)
	}
}
```

## 參考

### 來源與優先順序

```go
cfg, err := config.Load[Config](ctx, config.From(
	config.Profile("app"),                                 // configs/app.yaml + configs/app.$ENV.yaml
	config.File("/etc/app/extra.toml", config.Optional()), // .json .yaml .yml .toml；Optional：可以不存在
	config.DotEnv(".env", "APP", config.DotEnvOptional()), // 名稱規則同 Env
	etcd.Prefix(cli, "/order-service/prod/"),              // /order-service/prod/mysql/main/host -> mysql.main.host
	config.Env("APP"),                                     // 真正的環境變數
))
// 後面的來源蓋過前面的：map 逐個 key 合併，Named 的實例依名稱合併。
// 有 env tag 的欄位在所有來源之後才設定。
// 另外還有 config.Bytes("yaml", data) 和 config.Map(m)，給測試或程式內建的
// 預設值用。自訂來源實作 config.Source；要參與熱更新，再實作
// config.WatchableSource。
```

### 哪些來源會看 ENV

只有 `Profile` 會看 `ENV`。其他來源讀的都是你給的路徑、前綴或 key；要依環境切換，就把環境放進路徑或 key。

| 來源 | 會不會看 `ENV` | 怎麼依環境切換 |
|---|---|---|
| `config.Profile("app")` | 會：先讀 `app.yaml` 再讀 `app.$ENV.yaml`；`$CONFIG_PATH` 有值時只讀那個檔 | 檔名 |
| `config.File(path)` | 不會 | 自己組的路徑 |
| `config.Env("APP")`、`config.DotEnv` | 不會 | 各環境部署時給的值 |
| `etcd.Key`、`etcd.Prefix` | 不會 | key：`"/order-service/"+env+"/"` |
| `env` tag | 不會 | 各環境部署時給的值 |

### 環境變數的名稱

```bash
# config.Env("APP") 讀取 APP_ 開頭的變數。「__」分隔層級，單一個「_」留在 key 裡。
# 分隔符號可以用 config.EnvSeparator 改。
APP_PORT=9090                          # port
APP_MYSQL__MAIN__PASSWORD=s3cret       # mysql.main.password
APP_MYSQL__MAIN__CONN_MAX_LIFETIME=10m # mysql.main.conn_max_lifetime
APP_REDIS__ADDRS=r1:6379,r2:6379       # 清單：以逗號分隔
APP_MYSQL__REPORT__HOST=10.0.0.3       # 新增實例 report
APP_MYSQL__MAIN__PARAMS__PARSETIME=1   # map 的 key 進來會變成小寫：parsetime
# 空值視為沒設定。對不到欄位的變數直接忽略，不會有警告，WithStrict 也不例外：
# 環境裡本來就有很多無關的變數。
```

### Struct tag

```go
type Config struct {
	Port    int             `default:"8080" env:"PORT"` // env：在所有來源之後讀取 $PORT
	Timeout time.Duration   `default:"5s"`              // duration 要寫成字串："500ms"、"5m"
	Buffer  config.ByteSize `default:"64MiB"`           // 64MiB = 64×1024²，64MB = 64×1000²
	Hosts   []string        `validate:"min=1"`          // go-playground/validator 的規則
	Token   config.Secret   // 印出來是 ******；Token.Value() 才是原文
	GRPC    GRPCConfig      `env-prefix:"GRPC_"`        // GRPCConfig 裡 env tag 的前綴
	Legacy  string          `config:"legacy_name"`      // 指定 key；"-" 表示略過這個欄位
}
// 型別可以實作 SetDefaults()、Validate() error 或 UnmarshalConfig(any) error
// 介入載入流程。實作 encoding.TextUnmarshaler 的型別（例如 netip.Addr）可以
// 從字串解碼。config.WithTagName("yaml") 改讀 yaml tag，不讀 config tag。
```

### Open 與 New

```go
rdb, err := conn_redis.Open(ctx, cfg.Redis) // 建立後在 ping_timeout 內確認；失敗時先關閉再回傳錯誤
rdb, err := conn_redis.New(cfg.Redis)       // 只建立，不做網路 I/O；第一次使用時才連線
o, err := conn_redis.Options(cfg.Redis)     // driver 原生的設定，給自己建 client 時使用
```

driver 無法「只建立、不連線」時，connector 就沒有 `New`；HTTP API client 沒有連線可以確認，就沒有 `Open`。

| connector | Open | New | 轉成 driver 設定 |
|---|---|---|---|
| `conn_sql` | `OpenMySQL`、`OpenPostgres` | `NewMySQL`、`NewPostgres` | `MySQLConfig`、`PostgresConfig`、`PostgresConnString` |
| `conn_mssql` | `Open` | `New` | `Config`、`Connector` |
| `conn_oracle` | `Open` | `New` | `URL`、`Connector` |
| `conn_sqlite` | `Open` | `New` | `DSN` |
| `conn_trino` | `Open` | `New` | `DSN` |
| `conn_clickhouse` | `Open`（原生 API）、`OpenDB`（database/sql） | `New`、`NewDB` | `Options`、`Connector` |
| `conn_gorm` | `OpenMySQL`、`OpenPostgres` | 無：`gorm.Open` 初始化 dialector 時可能就會連線（MySQL 會查詢 server 版本） | |
| `conn_gorm_mssql`、`conn_gorm_sqlite` | `Open` | 無：同 `conn_gorm` | |
| `conn_redis` | `Open` | `New` | `Options` |
| `conn_mongo` | `Open`、`OpenDatabase` | `New` | `ClientOptions` |
| `conn_cassandra`、`conn_scylla` | `Open` | 無：gocql 建立 session 時就會連線 | `ClusterConfig` |
| `conn_es8`、`conn_es9` | `Open` | `New`、`NewTypedClient` | `Config` |
| `conn_opensearch` | `Open` | `New` | `Config` |
| `conn_kafka_franz` | `Open` | `New` | `Options` |
| `conn_kafka_sarama` | `OpenClient`、`OpenSyncProducer`、`OpenAsyncProducer`、`OpenConsumerGroup`、`OpenConsumer`、`OpenClusterAdmin` | 無：sarama 建立 client 時就會連線 | `Config` |
| `conn_mqtt` | `Open` | `New` | `ClientOptions` |
| `conn_mqtt_rest` | 無：HTTP API | `New`、`NewEMQX`、`NewGeneric` | |
| `conn_streamload` | 無：HTTP API | `New` | |

```go
// 每個入口都接受 option，處理設定檔表達不了的東西。
db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("main"),
	conn_gorm.WithGormConfig(&gorm.Config{PrepareStmt: true}))

// gocql 和 sarama 連線途中無法中斷。ctx 先結束時，Open 立刻回傳 ctx.Err()，
// 之後才建好的 session 或 client 會自動關閉。請給 ctx 期限，或用
// options.connect_timeout（gocql）、options.net.dial_timeout（sarama）
// 限制 driver 本身。
ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()
session, err := conn_cassandra.Open(ctx, cfg.Cassandra)
```

### Connector 一覽

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
pg, err := conn_sql.OpenPostgres(ctx, cfg.Postgres.MustGet("main"))    // 經由 pgx 的 *sql.DB
gdb, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("main"))        // *gorm.DB；沒設定 parseTime 時自動開啟
gpg, err := conn_gorm.OpenPostgres(ctx, cfg.Postgres.MustGet("main"))
mss, err := conn_mssql.Open(ctx, cfg.SQLServer.MustGet("erp"))
gmss, err := conn_gorm_mssql.Open(ctx, cfg.SQLServer.MustGet("erp"))
ora, err := conn_oracle.Open(ctx, cfg.Oracle.MustGet("core"))          // 純 Go，不需要 Instant Client
lite, err := conn_sqlite.Open(ctx, cfg.SQLite.MustGet("local"))        // 不需要 cgo
glite, err := conn_gorm_sqlite.Open(ctx, cfg.SQLite.MustGet("local"))
```

```yaml
# config.MySQL 也適用 TiDB、OceanBase，以及 Doris、StarRocks（連 FE 的 9030）。
# config.Postgres 也適用 CockroachDB、YugabyteDB、Redshift、TimescaleDB、Greenplum。
mysql:
  main:
    dsn: "app@tcp(db1:3306)/shop"   # 或 host、port、user、database
    password: s3cret                # 欄位會蓋過 DSN，例如從 secret store 取得的密碼
    params: {loc: Local}
    max_open_conns: 20
sqlserver:
  erp: {host: mssql, user: sa, password: pw, database: erp, params: {encrypt: "true"}}
oracle:
  core: {host: ora, service: FREEPDB1, user: app, password: pw}
sqlite:
  local: {path: data/app.db}        # 或 ":memory:"
```

#### NoSQL

```go
import (
	"github.com/linzeyan/loadconf/connector/conn_cassandra"
	"github.com/linzeyan/loadconf/connector/conn_mongo"
	"github.com/linzeyan/loadconf/connector/conn_redis"
	"github.com/linzeyan/loadconf/connector/conn_scylla"
)

rdb, err := conn_redis.Open(ctx, cfg.Redis)          // redis.UniversalClient：single、cluster 或 sentinel
mdb, err := conn_mongo.OpenDatabase(ctx, cfg.Mongo)  // *mongo.Database；conn_mongo.Open 回傳 client
cass, err := conn_cassandra.Open(ctx, cfg.Cassandra) // *gocql.Session
scy, err := conn_scylla.Open(ctx, cfg.Scylla)        // 加上下面的 replace 才有 shard-aware
```

```yaml
redis:
  addrs: ["r1:6379", "r2:6379", "r3:6379"]  # 多個位址：cluster；加上 master_name：sentinel
  password: s3cret
mongo:
  uri: "mongodb://m1,m2/?replicaSet=rs0"
  database: shop
scylla:
  hosts: [s1, s2]
  local_dc: dc1
  options: {consistency: LOCAL_QUORUM}      # gocql 要求大寫
```

```
// 使用 conn_scylla 的應用程式，自己的 go.mod 要加這行。沒加仍然能編譯、
// 能連線，但用的是上游 gocql：沒有 shard-aware，Open 也會記一筆警告。
replace github.com/gocql/gocql => github.com/scylladb/gocql v1.19.0
```

#### OLAP

```go
import (
	"github.com/linzeyan/loadconf/connector/conn_clickhouse"
	"github.com/linzeyan/loadconf/connector/conn_streamload"
	"github.com/linzeyan/loadconf/connector/conn_trino"
)

ch, err := conn_clickhouse.Open(ctx, cfg.ClickHouse)     // 原生 API：批次寫入最快
chDB, err := conn_clickhouse.OpenDB(ctx, cfg.ClickHouse) // database/sql
tr, err := conn_trino.Open(ctx, cfg.Trino)               // 用 SELECT 1 確認

// Doris、StarRocks 的大量寫入：用 Stream Load，不要用 INSERT。
// 請求失敗時換下一個 FE、以同一個 label 重試；server 依 label 去重，
// 所以同一批資料最多匯入一次。資料錯誤不重試，回傳帶 ErrorURL 的
// *conn_streamload.LoadError。
sl, err := conn_streamload.New(cfg.Doris) // cfg.Doris 的型別是 config.StreamLoad

res, err := sl.LoadJSON(ctx, "events", rows) // rows：slice，送出時是一個 JSON 陣列
res, err = sl.LoadCSV(ctx, "events", records, conn_streamload.LoadOptions{
	Headers: map[string]string{"columns": "id,name"},
})

w, err := sl.NewWriter("events") // 達到 max_rows、max_bytes 或 flush_interval 時送出
err = w.WriteJSON(ctx, event)
err = w.Close(ctx)

// 兩階段提交。Doris 一個交易只能 Load 一次，StarRocks 可以多次。
// 重試會重送資料，所以 reader 要能 Seek（*bytes.Reader、*os.File）。
tx, err := sl.Begin(ctx, "orders")
if _, err := tx.Load(ctx, bytes.NewReader(data)); err != nil {
	_ = tx.Abort(ctx)
} else {
	err = tx.Commit(ctx)
}
```

```yaml
doris:
  flavor: doris             # 或 starrocks
  addrs: ["fe1:8030", "fe2:8030"]
  database: dw
  password: pw
  headers: {strict_mode: "true"}
```

#### 搜尋

```go
import (
	"github.com/linzeyan/loadconf/connector/conn_es8" // 或 conn_es9
	"github.com/linzeyan/loadconf/connector/conn_opensearch"
)

es, err := conn_es8.Open(ctx, cfg.Elasticsearch)
typed, err := conn_es8.NewTypedClient(cfg.Elasticsearch)
osc, err := conn_opensearch.Open(ctx, cfg.OpenSearch)
```

```yaml
elasticsearch:
  addresses: ["https://es1:9200"]
  api_key: <key>            # 或 username 加 password、service_token、cloud_id
  options: {max_retries: 5, compress_request_body: true}
```

#### 訊息

```go
import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/linzeyan/loadconf/connector/conn_kafka_franz"
	"github.com/linzeyan/loadconf/connector/conn_kafka_sarama"
	"github.com/linzeyan/loadconf/connector/conn_mqtt"
	"github.com/linzeyan/loadconf/connector/conn_mqtt_rest"
)

// franz-go：同一個 client 負責 produce 和 consume；設定用 kgo option。
kc, err := conn_kafka_franz.Open(ctx, cfg.Kafka,
	kgo.DefaultProduceTopic("events"), kgo.ProducerBatchCompression(kgo.ZstdCompression()))

// sarama：設定寫在 options。producer 預設是 idempotent，並等待所有 in-sync replica 確認。
prod, err := conn_kafka_sarama.OpenSyncProducer(ctx, cfg.Kafka)
group, err := conn_kafka_sarama.OpenConsumerGroup(ctx, cfg.Kafka)
err = group.Consume(ctx, cfg.Kafka.Consumer.Topics, handler)

// MQTT 3.1.1（Paho）。設了 options.connect_retry 時 Open 會持續重試，請給 ctx 期限。
mc, err := conn_mqtt.Open(ctx, cfg.MQTT)

// EMQX REST API，支援 5.8 到 6.3。
emqx, err := conn_mqtt_rest.NewEMQX(cfg.EMQX)
res, err := emqx.Publish(ctx, conn_mqtt_rest.Message{Topic: "a/b", Payload: []byte("hi"), QoS: 1})
// res.NoSubscribers：已發佈，但沒有任何訂閱者
```

```yaml
kafka:
  brokers: ["k1:9092"]
  consumer: {group: order-service, topics: [orders], initial_offset: earliest}
mqtt:
  brokers: ["tcp://emqx:1883"]
  client_id: order-service
  client_id_random_suffix: true   # 同一服務的多個副本不能共用 client ID，否則會互相踢掉
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

log, err := logger.New(ctx, cfg.Log) // 內嵌 *slog.Logger
defer log.Close()                    // 送出 elasticsearch、otlp 輸出佇列裡剩下的紀錄
log.SetDefault()
// SetDefault 之後開的連線，driver 的 log 都會寫到 log：
//   gorm：失敗的查詢，以及超過 200ms 的查詢
//   MySQL、PostgreSQL：warn 以上
//   MongoDB、Cassandra／Scylla、ClickHouse、Kafka（franz-go）、Stream Load
//   Elasticsearch、OpenSearch：每個請求都記；4xx、5xx 記成 warn

zl, err := logger_zap.New(ctx, cfg.Log)     // 同一份設定與輸出，換成 zap
zr, err := logger_zerolog.New(ctx, cfg.Log) // 或 zerolog
```

```yaml
log:
  level: info                 # trace、debug、info、warn、error；熱更新可以改
  format: json                # 或 text：只限 stdout、stderr、file
  add_source: true
  service: order-service      # 加上 service 欄位
  fields: {env: prod}         # 每筆紀錄都帶的固定欄位
  outputs:                    # 沒寫時預設 stdout
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

// OTLP 因為 gRPC 依賴較重，放在獨立的 module，要自己傳進去。
log, err := logger.New(ctx, cfg.Log,
	logger.WithSink("otlp", sink_otlp.Open),
	logger.WithContextAttrs(sink_otlp.TraceAttrs)) // 從 ctx 加上 trace_id、span_id
log.InfoContext(ctx, "handled")

// driver 要用預設以外的 logger，就傳 WithLogger。gorm 的等級也可以寫在設定檔：
type Config struct {
	GormLog conn_gorm_log.Config // slow_threshold、level、parameterized_queries
}
gdb, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("main"),
	conn_gorm.WithGormConfig(&gorm.Config{Logger: conn_gorm_log.New(nil, cfg.GormLog)}))
es, err := conn_es8.Open(ctx, cfg.Elasticsearch, conn_es8.WithLogger(auditLog))

// 這些 driver 只有整個 process 共用的 logger，要自己設定一次。
redis.SetLogger(conn_redis.SlogLogger(log.Logger, slog.LevelWarn))
sarama.Logger = conn_kafka_sarama.SlogLogger(log.Logger, slog.LevelInfo)
mssql.SetContextLogger(conn_mssql.SlogLogger(log.Logger))
restore := conn_mqtt.SetLogger(log.Logger)
```

## 版本

```bash
# 所有 module 用同一個版本號發佈，升級時一起升。
go get github.com/linzeyan/loadconf/config@v0.3.0 github.com/linzeyan/loadconf/connector/conn_gorm@v0.3.0
# 混用不同版本也能編譯，但 Go 會替共用的 module（例如 config）選用所有 module
# 要求的最高版本，所以釘在舊版並不會生效。
```

開發、測試與發版見 [CONTRIBUTING.md](CONTRIBUTING.md)。
