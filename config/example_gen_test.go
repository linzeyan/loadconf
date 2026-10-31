package config

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
)

// example.yaml and schema.json are generated from the settings types, so they
// cannot drift from the code: TestExampleFiles fails when they are stale and
// rewrites them with -update (make generate). Chinese comments and example
// values come from the docs table below; a field without an entry, or an
// entry that matches no field, fails the generator.

var updateExample = flag.Bool("update", false, "rewrite example.yaml and schema.json")

const (
	exampleFile = "example.yaml"
	schemaFile  = "schema.json"
	schemaID    = "https://raw.githubusercontent.com/linzeyan/loadconf/main/config/schema.json"
	customFile  = "example_custom_test.go"
)

// fieldDoc documents one key, by owner type and key, for both files.
type fieldDoc struct {
	zh   string   // comment in example.yaml and description in schema.json
	ex   any      // example value; the default or zero value when nil
	enum []string // allowed values, for editor completion
	src  []string // custom sections: declarations example.yaml prints
}

// example overrides the docs table for one section or Named instance, as
// sections of the same type share the table.
type example struct {
	zh   string         // comment above a Named instance
	set  map[string]any // values by dotted key path
	only []string       // keys shown; all when nil
}

type namedItem struct {
	name string
	example
}

// namedExample lists the instances of a Named field; "main" when absent.
type namedExample struct {
	list  bool // write the list form instead of the mapping form
	items []namedItem
}

// entries is a mapping written one key per line, each with its comment.
type entries []entry

type entry struct {
	key, zh string
	val     any
}

const (
	dsnNote    = "完整連線字串，與 host 等欄位擇一；兩者都填時，欄位覆蓋連線字串中的對應部分。"
	pingNote   = "開啟連線時連通性檢查的逾時。"
	secretNote = "密碼（config.Secret，印出時遮蔽）；正式環境建議以環境變數覆蓋。"
	tlsNote    = "TLS 設定。"
	optsNote   = "驅動程式的其他選項：鍵是驅動設定結構的欄位名稱轉成 snake_case，"
)

var (
	schemaLevels = []string{"trace", "debug", "info", "warn", "error", "fatal"}
	stackLevels  = append(slices.Clone(schemaLevels), "none")
)

var docs = map[string]fieldDoc{
	// Sections of AppConfig.
	"AppConfig.log":           {zh: "日誌：logger、logger/logger_zap、logger/logger_zerolog 讀這個區段。"},
	"AppConfig.mysql":         {zh: "MySQL（conn_sql、conn_gorm）。config.Named 可放多個實例，程式以 cfg.MySQL.Get(\"main\") 取用。"},
	"AppConfig.postgres":      {zh: "PostgreSQL（conn_sql、conn_gorm）。"},
	"AppConfig.sqlserver":     {zh: "SQL Server（conn_mssql、conn_gorm_mssql）。"},
	"AppConfig.oracle":        {zh: "Oracle（conn_oracle，go-ora）。"},
	"AppConfig.sqlite":        {zh: "SQLite（conn_sqlite、conn_gorm_sqlite）。"},
	"AppConfig.redis":         {zh: "Redis（conn_redis）：單機、cluster 或 sentinel。"},
	"AppConfig.mongo":         {zh: "MongoDB（conn_mongo）。"},
	"AppConfig.cassandra":     {zh: "Cassandra（conn_cassandra）。"},
	"AppConfig.scylla":        {zh: "ScyllaDB（conn_scylla），欄位與 cassandra 相同。"},
	"AppConfig.clickhouse":    {zh: "ClickHouse（conn_clickhouse）。"},
	"AppConfig.doris":         {zh: "Doris／StarRocks Stream Load（conn_streamload）。"},
	"AppConfig.trino":         {zh: "Trino（conn_trino）。"},
	"AppConfig.elasticsearch": {zh: "Elasticsearch（conn_es8、conn_es9）。"},
	"AppConfig.opensearch":    {zh: "OpenSearch（conn_opensearch），欄位與 elasticsearch 相同。"},
	"AppConfig.kafka":         {zh: "Kafka（conn_kafka_franz、conn_kafka_sarama）。"},
	"AppConfig.mqtt":          {zh: "MQTT broker 連線（conn_mqtt，paho）。"},
	"AppConfig.emqx":          {zh: "經 HTTP API 發佈 MQTT 訊息（conn_mqtt_rest）：EMQX 或自訂閘道。"},
	"AppConfig.server": {
		zh:  "自訂一：應用程式自己的欄位，用 default 給預設值、validate 標籤檢查。\n時間與大小的規則直接寫單位，例如 gte=1s、lte=32MiB。",
		src: []string{"Server"},
	},
	"AppConfig.tenants": {
		zh:  "自訂二：自己的型別放進 config.Named，這裡用清單形式（每項帶 name）。\n跨區段的檢查寫在根結構的 Validate 方法：每個租戶的 database 必須是 mysql 的實例。",
		src: []string{"Tenant", "AppConfig.Validate"},
	},
	"AppConfig.rate_limits": {
		zh:  "自訂三：型別實作 UnmarshalConfig，自己決定接受哪些寫法。",
		src: []string{"RateLimit", "RateLimit.UnmarshalConfig"},
		ex: entries{
			{"api", "字串形式：每秒 100 次。", "100/s"},
			{"search", "每 30 秒 20 次。", "20/30s"},
			{"login", "對映形式。", yaml.MapSlice{{Key: "requests", Value: 5}, {Key: "per", Value: "1m"}}},
		},
	},

	// Shared parts.
	"TLS.enabled":                {zh: "啟用 TLS。其餘欄位只在啟用時生效。"},
	"TLS.ca_file":                {zh: "信任的 CA（PEM）；空值用系統根憑證。"},
	"TLS.cert_file":              {zh: "mTLS 用戶端憑證（PEM），與 key_file 成對設定。"},
	"TLS.key_file":               {zh: "mTLS 用戶端私鑰（PEM）。"},
	"TLS.server_name":            {zh: "驗證伺服器憑證用的主機名稱；空值取連線位址。"},
	"TLS.insecure_skip_verify":   {zh: "略過伺服器憑證驗證，僅限測試環境。"},
	"TLS.min_version":            {zh: "最低 TLS 版本，空值為 1.2。", ex: "1.2", enum: []string{"1.2", "1.3"}},
	"SQLPool.max_open_conns":     {zh: "連線池：最大開啟連線數，0 為不限。", ex: 20},
	"SQLPool.max_idle_conns":     {zh: "連線池：最大閒置連線數，不可超過 max_open_conns。", ex: 10},
	"SQLPool.conn_max_lifetime":  {zh: "連線池：連線最長存活時間，0 為不限。", ex: "1h"},
	"SQLPool.conn_max_idle_time": {zh: "連線池：連線最長閒置時間，0 為不限。", ex: "10m"},

	"MySQL.dsn":          {zh: dsnNote + "\n例如 app:pw@tcp(mysql:3306)/orders?parseTime=true"},
	"MySQL.host":         {zh: "主機名稱或 IP。", ex: "mysql"},
	"MySQL.port":         {zh: "連接埠。"},
	"MySQL.user":         {zh: "使用者。", ex: "app"},
	"MySQL.password":     {zh: secretNote, ex: "change-me"},
	"MySQL.database":     {zh: "資料庫名稱。", ex: "orders"},
	"MySQL.params":       {zh: "go-sql-driver 的 DSN 參數，例如 parseTime、loc、charset、timeout。", ex: map[string]string{"parseTime": "true", "loc": "Local"}},
	"MySQL.ping_timeout": {zh: pingNote},
	"MySQL.tls":          {zh: tlsNote},

	"Postgres.dsn":          {zh: dsnNote + "\nURL（postgres://app:pw@postgres:5432/orders）或 key=value 形式。"},
	"Postgres.host":         {zh: "主機名稱或 IP。", ex: "postgres"},
	"Postgres.port":         {zh: "連接埠。"},
	"Postgres.user":         {zh: "使用者。", ex: "app"},
	"Postgres.password":     {zh: secretNote, ex: "change-me"},
	"Postgres.database":     {zh: "資料庫名稱。", ex: "orders"},
	"Postgres.params":       {zh: "libpq 連線參數（sslmode、connect_timeout）或執行期參數（search_path、application_name、statement_timeout）。", ex: map[string]string{"sslmode": "disable", "application_name": "orders"}},
	"Postgres.ping_timeout": {zh: pingNote},
	"Postgres.tls":          {zh: "TLS 設定；啟用時取代 sslmode 推導出的 TLS 設定。"},

	"SQLServer.dsn":          {zh: dsnNote + "\n例如 sqlserver://app:pw@sqlserver:1433?database=orders"},
	"SQLServer.host":         {zh: "主機名稱或 IP。", ex: "sqlserver"},
	"SQLServer.port":         {zh: "連接埠；設了 instance 時忽略。"},
	"SQLServer.instance":     {zh: "具名執行個體，經 SQL Server Browser 解析連接埠。"},
	"SQLServer.user":         {zh: "使用者。", ex: "app"},
	"SQLServer.password":     {zh: secretNote, ex: "change-me"},
	"SQLServer.database":     {zh: "資料庫名稱。", ex: "orders"},
	"SQLServer.params":       {zh: "go-mssqldb 連線參數，例如 app name、dial timeout（秒）、encrypt（disable、false、true、strict）。\n啟用 TLS 時 encrypt 未設定則視為 true。", ex: map[string]string{"app name": "orders", "encrypt": "true"}},
	"SQLServer.ping_timeout": {zh: pingNote},
	"SQLServer.tls":          {zh: "憑證驗證設定；CA、server_name 與用戶端憑證在加密開啟時生效。"},

	"Oracle.dsn":            {zh: "完整連線字串，例如 oracle://app:pw@oracle:1521/ORCLPDB1。dsn、connect_string、host 三擇一。"},
	"Oracle.host":           {zh: "主機名稱或 IP。", ex: "oracle"},
	"Oracle.port":           {zh: "連接埠。"},
	"Oracle.service":        {zh: "服務名稱；與 sid 擇一。", ex: "ORCLPDB1"},
	"Oracle.sid":            {zh: "舊式系統識別碼（SID）；與 service 擇一。"},
	"Oracle.connect_string": {zh: "EZConnect 字串或完整 TNS 描述，取代 host、port、service、sid，用於 RAC 或 failover。"},
	"Oracle.user":           {zh: "使用者。", ex: "app"},
	"Oracle.password":       {zh: secretNote, ex: "change-me"},
	"Oracle.params":         {zh: "go-ora URL 選項，例如 CONNECTION TIMEOUT（秒）、PREFETCH_ROWS、WALLET。", ex: map[string]string{"CONNECTION TIMEOUT": "30"}},
	"Oracle.ping_timeout":   {zh: pingNote},
	"Oracle.tls":            {zh: "啟用 TCPS；go-ora 以 wallet（params 的 WALLET）或系統根憑證驗證。"},

	"SQLite.path":         {zh: "檔案路徑、file: URI 或 :memory:。記憶體資料庫只屬於一條連線，連線池會限制為 1。", ex: "data/app.db"},
	"SQLite.mode":         {zh: "開啟模式：ro 唯讀、rw 讀寫、rwc 讀寫且不存在時建立（空值即 rwc）、memory。", ex: "rwc", enum: []string{"ro", "rw", "rwc", "memory"}},
	"SQLite.busy_timeout": {zh: "等待鎖的時間，逾時回傳 SQLITE_BUSY。"},
	"SQLite.journal_mode": {zh: "日誌模式；空值保留資料庫原本的設定。", enum: []string{"delete", "truncate", "persist", "memory", "wal", "off"}},
	"SQLite.synchronous":  {zh: "同步模式；空值為 full，WAL 下用 normal 安全且較快。", ex: "normal", enum: []string{"off", "normal", "full", "extra"}},
	"SQLite.foreign_keys": {zh: "啟用外鍵約束。"},
	"SQLite.tx_lock":      {zh: "交易的 BEGIN 模式；空值為 deferred。immediate 可避免讀交易之後寫入時的 SQLITE_BUSY。", ex: "immediate", enum: []string{"deferred", "immediate", "exclusive"}},
	"SQLite.pragmas":      {zh: "每條連線都套用的其他 PRAGMA。", ex: map[string]string{"cache_size": "-20000"}},

	"Redis.mode":              {zh: "single、cluster 或 sentinel；空值時推斷：有 master_name 為 sentinel，多個 addrs 為 cluster，否則 single。", ex: "single", enum: []string{"single", "cluster", "sentinel"}},
	"Redis.addrs":             {zh: "host:port 清單：單機一個位址、cluster 的種子節點，或 sentinel 節點。", ex: []string{"redis:6379"}},
	"Redis.master_name":       {zh: "sentinel 模式必填的 master 名稱。"},
	"Redis.username":          {zh: "ACL 使用者；空值為 default。"},
	"Redis.password":          {zh: secretNote, ex: "change-me"},
	"Redis.db":                {zh: "資料庫編號；cluster 模式只能是 0。"},
	"Redis.sentinel_username": {zh: "連 sentinel 節點的使用者。"},
	"Redis.sentinel_password": {zh: "連 sentinel 節點的密碼。"},
	"Redis.ping_timeout":      {zh: pingNote},
	"Redis.tls":               {zh: tlsNote},
	"Redis.options":           {zh: optsNote + "\n對應 go-redis 的 UniversalOptions，例如 pool_size、read_timeout。", ex: map[string]any{"pool_size": 20}},

	"Mongo.uri":            {zh: "mongodb:// 或 mongodb+srv:// 連線字串；與 hosts 擇一。"},
	"Mongo.hosts":          {zh: "host:port 清單。", ex: []string{"mongo:27017"}},
	"Mongo.database":       {zh: "預設資料庫。", ex: "orders"},
	"Mongo.username":       {zh: "使用者。", ex: "app"},
	"Mongo.password":       {zh: secretNote, ex: "change-me"},
	"Mongo.auth_source":    {zh: "驗證用的資料庫，常見為 admin。", ex: "admin"},
	"Mongo.auth_mechanism": {zh: "驗證機制，例如 SCRAM-SHA-256；空值由驅動協商。", ex: "SCRAM-SHA-256"},
	"Mongo.params":         {zh: "連線字串選項，例如 replicaSet、appName、readPreference、maxPoolSize；覆蓋 uri 中的同名選項。", ex: map[string]string{"appName": "orders"}},
	"Mongo.ping_timeout":   {zh: pingNote},
	"Mongo.tls":            {zh: tlsNote},

	"Cassandra.hosts":       {zh: "contact points，host 或 host:port。", ex: []string{"cassandra1", "cassandra2"}},
	"Cassandra.port":        {zh: "hosts 沒寫連接埠時使用的連接埠。"},
	"Cassandra.keyspace":    {zh: "keyspace。", ex: "orders"},
	"Cassandra.username":    {zh: "使用者。", ex: "app"},
	"Cassandra.password":    {zh: secretNote, ex: "change-me"},
	"Cassandra.local_dc":    {zh: "優先使用的資料中心，啟用 DC-aware、token-aware 路由；多資料中心叢集建議設定。", ex: "dc1"},
	"Cassandra.compression": {zh: "壓縮：snappy、lz4，空值不壓縮。", ex: "snappy", enum: []string{"snappy", "lz4"}},
	"Cassandra.retries":     {zh: "簡單重試策略的重試次數。", ex: 3},
	"Cassandra.tls":         {zh: tlsNote},
	"Cassandra.options":     {zh: optsNote + "\n對應 gocql 的 ClusterConfig，例如 num_conns、timeout。", ex: map[string]any{"num_conns": 2}},

	"ClickHouse.dsn":          {zh: "完整連線字串，例如 clickhouse://default:pw@clickhouse:9000/default；與 addrs 擇一。"},
	"ClickHouse.addrs":        {zh: "host:port 清單：native 用 9000（TLS 9440），http 用 8123（TLS 8443）。", ex: []string{"clickhouse:9000"}},
	"ClickHouse.protocol":     {zh: "native 或 http。", enum: []string{"native", "http"}},
	"ClickHouse.database":     {zh: "預設資料庫。", ex: "default"},
	"ClickHouse.username":     {zh: "使用者。", ex: "default"},
	"ClickHouse.password":     {zh: secretNote},
	"ClickHouse.compression":  {zh: "壓縮方式；gzip、deflate、br 只限 http。等級用 options.compression.level 設定。", ex: "lz4", enum: []string{"none", "lz4", "lz4hc", "zstd", "gzip", "deflate", "br"}},
	"ClickHouse.ping_timeout": {zh: pingNote},
	"ClickHouse.tls":          {zh: tlsNote},
	"ClickHouse.options":      {zh: optsNote + "\n對應 clickhouse-go 的 Options，例如 dial_timeout、settings。", ex: map[string]any{"dial_timeout": "5s"}},

	"StreamLoad.flavor":              {zh: "doris 或 starrocks。", enum: []string{"doris", "starrocks"}},
	"StreamLoad.addrs":               {zh: "FE 的 HTTP 端點，host:port 或 http(s)://host:port（預設連接埠 8030）；輪流使用。", ex: []string{"doris-fe:8030"}},
	"StreamLoad.username":            {zh: "使用者。"},
	"StreamLoad.password":            {zh: secretNote},
	"StreamLoad.database":            {zh: "資料庫（必填）。", ex: "orders"},
	"StreamLoad.table":               {zh: "預設寫入的資料表。", ex: "events"},
	"StreamLoad.format":              {zh: "資料格式：json 或 csv。", enum: []string{"json", "csv"}},
	"StreamLoad.headers":             {zh: "每次載入都帶上的屬性，例如 column_separator、strict_mode、timezone、max_filter_ratio。", ex: map[string]string{"strict_mode": "true"}},
	"StreamLoad.label_prefix":        {zh: "自動產生的 load label 前綴。", ex: "orders"},
	"StreamLoad.timeout":             {zh: "單次 HTTP 載入請求的逾時。"},
	"StreamLoad.max_retries":         {zh: "失敗時以同一個 label 重試的次數；伺服器依 label 去重。"},
	"StreamLoad.retry_backoff":       {zh: "重試間隔。"},
	"StreamLoad.tls":                 {zh: tlsNote},
	"StreamLoad.batch":               {zh: "批次寫入：任一門檻達到即送出。"},
	"StreamLoadBatch.max_rows":       {zh: "每批最多列數。"},
	"StreamLoadBatch.max_bytes":      {zh: "每批最大位元組數。"},
	"StreamLoadBatch.flush_interval": {zh: "最長等待時間，到了就送出未滿的批次。"},

	"Trino.dsn":          {zh: "完整連線字串，例如 https://app@trino:8443?catalog=hive；與 host 等欄位擇一。"},
	"Trino.host":         {zh: "主機名稱或 IP。", ex: "trino"},
	"Trino.port":         {zh: "連接埠。"},
	"Trino.user":         {zh: "使用者（沒有 dsn 時必填）。", ex: "app"},
	"Trino.password":     {zh: "密碼；需要 tls.enabled。與 access_token 擇一。"},
	"Trino.access_token": {zh: "JWT；需要 tls.enabled。與 password 擇一。"},
	"Trino.catalog":      {zh: "預設 catalog。", ex: "hive"},
	"Trino.schema":       {zh: "預設 schema。", ex: "default"},
	"Trino.params":       {zh: "trino-go-client 的 DSN 參數，例如 source、clientTags、session_properties、query_timeout。", ex: map[string]string{"source": "orders"}},
	"Trino.ping_timeout": {zh: pingNote},
	"Trino.tls":          {zh: tlsNote},

	"Elasticsearch.addresses":               {zh: "節點 URL 清單；與 cloud_id 擇一。", ex: []string{"https://es1:9200", "https://es2:9200"}},
	"Elasticsearch.cloud_id":                {zh: "Elastic Cloud 的 Cloud ID；與 addresses 擇一。"},
	"Elasticsearch.username":                {zh: "使用者；username、api_key、service_token 三擇一。", ex: "elastic"},
	"Elasticsearch.password":                {zh: secretNote, ex: "change-me"},
	"Elasticsearch.api_key":                 {zh: "base64 編碼的 id:api_key。"},
	"Elasticsearch.service_token":           {zh: "service account token。"},
	"Elasticsearch.certificate_fingerprint": {zh: "CA 憑證的 SHA-256 指紋（hex），Elasticsearch 8 第一次啟動時印出。"},
	"Elasticsearch.response_header_timeout": {zh: "每個請求等待回應標頭的逾時；0 為只受請求 context 限制。"},
	"Elasticsearch.ping_timeout":            {zh: pingNote},
	"Elasticsearch.tls":                     {zh: tlsNote},
	"Elasticsearch.options":                 {zh: optsNote + "\n對應 go-elasticsearch／opensearch-go 的 Config，例如 max_retries、compress_request_body。", ex: map[string]any{"max_retries": 3}},

	"Kafka.brokers":                {zh: "broker 的 host:port 清單。", ex: []string{"kafka1:9092", "kafka2:9092"}},
	"Kafka.client_id":              {zh: "client id。", ex: "orders"},
	"Kafka.version":                {zh: "broker 協定版本。sarama 需要它才能用新功能；franz-go 自行協商，只把它當上限。", ex: "3.6.0"},
	"Kafka.ping_timeout":           {zh: pingNote},
	"Kafka.sasl":                   {zh: "SASL 驗證；mechanism 留空為不驗證。"},
	"Kafka.tls":                    {zh: tlsNote},
	"Kafka.consumer":               {zh: "消費者設定。"},
	"Kafka.options":                {zh: optsNote + "\n只有 sarama 使用，對應 sarama.Config（例如 net.dial_timeout）；franz-go 必須留空。", ex: map[string]any{"net": map[string]any{"dial_timeout": "10s"}}},
	"KafkaSASL.mechanism":          {zh: "驗證機制。", ex: "SCRAM-SHA-512", enum: []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}},
	"KafkaSASL.username":           {zh: "使用者。", ex: "app"},
	"KafkaSASL.password":           {zh: secretNote, ex: "change-me"},
	"KafkaConsumer.group":          {zh: "consumer group。", ex: "orders"},
	"KafkaConsumer.topics":         {zh: "訂閱的 topic。", ex: []string{"orders.created"}},
	"KafkaConsumer.initial_offset": {zh: "沒有已提交 offset 時從哪裡開始：earliest 或 latest。", enum: []string{"earliest", "latest"}},

	"MQTT.brokers":                 {zh: "broker URL：tcp://、ssl://、ws:// 或 wss://。", ex: []string{"tcp://mqtt:1883"}},
	"MQTT.client_id":               {zh: "client id。", ex: "orders"},
	"MQTT.client_id_random_suffix": {zh: "在 client_id 後加隨機字尾，避免同一服務的多個副本互踢。", ex: true},
	"MQTT.username":                {zh: "使用者。", ex: "app"},
	"MQTT.password":                {zh: secretNote, ex: "change-me"},
	"MQTT.tls":                     {zh: tlsNote},
	"MQTT.options":                 {zh: optsNote + "\n對應 paho 的 ClientOptions，例如 auto_reconnect、keep_alive（秒）。", ex: map[string]any{"auto_reconnect": true}},

	"MQTTREST.provider":                {zh: "emqx 或 generic（自訂 HTTP 閘道）。", enum: []string{"emqx", "generic"}},
	"MQTTREST.base_url":                {zh: "API 根網址；EMQX 會自動加上 /api/v5。", ex: "http://emqx:18083"},
	"MQTTREST.auth":                    {zh: "HTTP 驗證。"},
	"MQTTREST.headers":                 {zh: "每個請求都帶上的 HTTP 標頭。"},
	"MQTTREST.timeout":                 {zh: "單一請求的逾時。"},
	"MQTTREST.max_retries":             {zh: "冪等請求（GET、DELETE）遇到網路錯誤、429、5xx 時的重試次數；發佈永不重試。"},
	"MQTTREST.retry_backoff":           {zh: "重試間隔。"},
	"MQTTREST.tls":                     {zh: tlsNote},
	"MQTTREST.publish":                 {zh: "generic 的發佈端點與請求本文欄位；emqx 不需要。"},
	"HTTPAuth.type":                    {zh: "none、basic、bearer 或 login（僅 emqx）；空值時有 username 為 basic，有 token 為 bearer。", ex: "basic", enum: []string{"none", "basic", "bearer", "login"}},
	"HTTPAuth.username":                {zh: "basic 或 login 的使用者；EMQX 用 API key。", ex: "orders-key"},
	"HTTPAuth.password":                {zh: "basic 或 login 的密碼；EMQX 用 API secret。", ex: "change-me"},
	"HTTPAuth.token":                   {zh: "bearer token。"},
	"MQTTRESTPublish.method":           {zh: "HTTP 方法。"},
	"MQTTRESTPublish.path":             {zh: "發佈路徑，generic 必填，例如 /publish。"},
	"MQTTRESTPublish.batch_path":       {zh: "接受 JSON 陣列的批次路徑；空值時批次逐筆送出。"},
	"MQTTRESTPublish.topic_field":      {zh: "請求本文中 topic 的欄位名稱。"},
	"MQTTRESTPublish.payload_field":    {zh: "請求本文中 payload 的欄位名稱。"},
	"MQTTRESTPublish.qos_field":        {zh: "請求本文中 QoS 的欄位名稱。"},
	"MQTTRESTPublish.retain_field":     {zh: "請求本文中 retain 的欄位名稱。"},
	"MQTTRESTPublish.payload_encoding": {zh: "payload 編碼：plain 或 base64。", enum: []string{"plain", "base64"}},
	"MQTTRESTPublish.encoding_field":   {zh: "設定時，本文多一個欄位帶 payload_encoding 的值。"},
	"MQTTRESTPublish.extra":            {zh: "每個請求本文都加上的固定欄位。"},

	"Log.level":       {zh: "最低等級，可在熱重載時變更。", enum: schemaLevels},
	"Log.format":      {zh: "stdout、stderr、file 輸出的預設格式：json 或 text。", enum: []string{"json", "text"}},
	"Log.add_source":  {zh: "加上呼叫處的檔案與行號。"},
	"Log.stack_level": {zh: "此等級以上附堆疊；none 關閉。", enum: stackLevels},
	"Log.time_format": {zh: "時間格式：rfc3339、rfc3339milli（預設）、rfc3339nano、unix、unixmilli、unixnano，或 Go layout。", ex: "rfc3339milli"},
	"Log.utc":         {zh: "時間用 UTC 而不是本地時區。"},
	"Log.pretty_json": {zh: "stdout、stderr、file 的 JSON 縮排輸出，供本機開發。"},
	"Log.service":     {zh: "加上 service 欄位，也是 syslog、GELF、OTLP 的程序名稱；空值為執行檔名稱。", ex: "orders"},
	"Log.hostname":    {zh: "加上 host 欄位。", ex: true},
	"Log.fields":      {zh: "每筆紀錄都加上的固定欄位，例如 env、version。", ex: map[string]string{"env": "production"}},
	"Log.outputs":     {zh: "輸出目的地，以名稱為鍵；沒有設定時輸出到 stdout。\ntype 預設等於名稱，所以名為 file 的輸出可以省略 type。"},

	"LogOutput.name":           {zh: "輸出名稱，即 outputs 的鍵。"},
	"LogOutput.type":           {zh: "stdout、stderr、file、syslog、gelf、elasticsearch、otlp，或以 logger.WithSink 註冊的自訂類型；空值等於名稱。"},
	"LogOutput.level":          {zh: "此輸出的最低等級；與 log.level 取較高者。", enum: schemaLevels},
	"LogOutput.format":         {zh: "覆蓋 log.format（stdout、stderr、file）；其他輸出一律 JSON。", enum: []string{"json", "text"}},
	"LogOutput.disabled":       {zh: "關閉此輸出，例如只在某個環境關掉。"},
	"LogOutput.path":           {zh: "日誌檔路徑（file 必填）。"},
	"LogOutput.max_size":       {zh: "超過此大小就輪替（file）。"},
	"LogOutput.max_backups":    {zh: "保留的輪替檔數量，0 為全部保留（file）。"},
	"LogOutput.max_age":        {zh: "輪替檔保留時間，0 為不限（file）。"},
	"LogOutput.local_time":     {zh: "輪替檔名用本地時間而不是 UTC（file）。"},
	"LogOutput.compress":       {zh: "gzip 壓縮：輪替檔（file）、UDP 封包（gelf）、請求本文（elasticsearch、otlp）。"},
	"LogOutput.network":        {zh: "syslog：udp、tcp、unix，空值為本機 syslog daemon；gelf：udp（預設）或 tcp。"},
	"LogOutput.addr":           {zh: "host:port（syslog、gelf）或 socket 路徑（unix syslog）；設了 network 就必填。"},
	"LogOutput.facility":       {zh: "syslog facility，例如 local0（預設）、user、daemon。"},
	"LogOutput.tag":            {zh: "syslog 的 app name；空值為 log.service。"},
	"LogOutput.syslog_format":  {zh: "rfc5424（預設）或 rfc3164。", enum: []string{"rfc5424", "rfc3164"}},
	"LogOutput.chunk_size":     {zh: "超過此大小的 UDP 封包會分塊（gelf，預設 1420）。"},
	"LogOutput.addresses":      {zh: "節點 URL（elasticsearch 必填）。"},
	"LogOutput.username":       {zh: "使用者；與 api_key 擇一（elasticsearch）。"},
	"LogOutput.password":       {zh: secretNote},
	"LogOutput.api_key":        {zh: "API key；與 username 擇一（elasticsearch）。"},
	"LogOutput.index":          {zh: "目標 index 或 data stream；{service} 換成 log.service，{date:LAYOUT} 換成紀錄日期（elasticsearch）。"},
	"LogOutput.endpoint":       {zh: "collector 的 host:port 或 URL；空值讀 OTEL_EXPORTER_OTLP_* 環境變數（otlp）。"},
	"LogOutput.protocol":       {zh: "grpc（預設）或 http（otlp）。", enum: []string{"grpc", "http"}},
	"LogOutput.insecure":       {zh: "不用 TLS 連 collector（otlp）。"},
	"LogOutput.headers":        {zh: "每個請求帶上的標頭（elasticsearch、otlp）。"},
	"LogOutput.timeout":        {zh: "單次寫入或匯出的逾時（syslog、gelf、elasticsearch、otlp）。"},
	"LogOutput.batch_size":     {zh: "非同步送出的批次大小（elasticsearch、otlp）。"},
	"LogOutput.flush_interval": {zh: "未滿的批次最長等待時間（elasticsearch、otlp）。"},
	"LogOutput.queue_size":     {zh: "佇列長度；佇列滿時丟棄紀錄（elasticsearch、otlp）。"},
	"LogOutput.tls":            {zh: "TLS：tcp 上的 syslog、gelf，以及 elasticsearch、otlp。"},
	"LogOutput.options":        {zh: "自訂類型輸出的設定。"},

	// Custom sections.
	"Server.listen":        {zh: "監聽位址，host:port；host 留空表示所有介面。"},
	"Server.public_url":    {zh: "對外網址；可留空，填了就必須是 URL。", ex: "https://orders.example.com"},
	"Server.mode":          {zh: "debug 或 release。"},
	"Server.read_timeout":  {zh: "讀取請求的逾時，限制在 1s 到 1m。"},
	"Server.max_body_size": {zh: "請求本文上限，最多 32MiB。", ex: "8MiB"},
	"Server.trusted_cidrs": {zh: "信任的代理網段；dive 讓 cidr 規則套用到每一項。", ex: []string{"10.0.0.0/8", "192.168.0.0/16"}},
	"Tenant.name":          {zh: "租戶名稱，由 Named 自動填入。"},
	"Tenant.database":      {zh: "租戶資料所在的 mysql 實例名稱（必填）。"},
	"Tenant.quota":         {zh: "每日配額，至少 1。"},
	"Tenant.features":      {zh: "啟用的功能。"},
	"Tenant.api_key":       {zh: "租戶的 API key（config.Secret）。"},
}

var sectionExamples = map[string]example{
	"scylla":     {set: map[string]any{"hosts": []string{"scylla1", "scylla2"}}},
	"opensearch": {set: map[string]any{"addresses": []string{"https://opensearch:9200"}, "username": "admin"}},
}

var namedExamples = map[string]namedExample{
	"log.outputs": {items: []namedItem{
		{"console", example{
			zh:   "標準輸出，文字格式。",
			only: []string{"type", "level", "format", "disabled"},
			set:  map[string]any{"type": "stdout", "level": "debug", "format": "text"},
		}},
		{"errors", example{
			zh:   "標準錯誤，只收 error 以上。",
			only: []string{"type", "level"},
			set:  map[string]any{"type": "stderr", "level": "error"},
		}},
		{"file", example{
			zh:   "日誌檔，依大小輪替。",
			only: []string{"type", "path", "max_size", "max_backups", "max_age", "local_time", "compress"},
			set:  map[string]any{"type": "file", "path": "logs/app.log", "max_backups": 7, "max_age": "168h", "compress": true},
		}},
		{"syslog", example{
			only: []string{"type", "network", "addr", "facility", "tag", "syslog_format", "timeout"},
			set:  map[string]any{"type": "syslog", "network": "udp", "addr": "syslog:514", "facility": "local0", "tag": "orders", "syslog_format": "rfc5424"},
		}},
		{"graylog", example{
			zh:   "GELF，送往 Graylog。",
			only: []string{"type", "network", "addr", "chunk_size", "compress"},
			set:  map[string]any{"type": "gelf", "network": "udp", "addr": "graylog:12201", "chunk_size": 1420, "compress": true},
		}},
		{"elasticsearch", example{
			only: []string{"type", "addresses", "username", "password", "api_key", "index", "headers", "timeout", "batch_size", "flush_interval", "queue_size", "tls"},
			set:  map[string]any{"type": "elasticsearch", "addresses": []string{"https://es1:9200"}, "username": "elastic", "password": "change-me", "index": "logs-{service}-default"},
		}},
		{"otlp", example{
			zh:   "OpenTelemetry collector。",
			only: []string{"type", "endpoint", "protocol", "insecure", "headers", "timeout", "batch_size", "flush_interval", "queue_size"},
			set:  map[string]any{"type": "otlp", "endpoint": "otel-collector:4317", "protocol": "grpc", "insecure": true},
		}},
		{"alerts", example{
			zh:   "自訂類型：type 是以 logger.WithSink 註冊的名稱，設定放在 options。這裡先關閉。",
			only: []string{"type", "level", "disabled", "options"},
			set:  map[string]any{"type": "slack", "level": "error", "disabled": true, "options": map[string]any{"webhook_url": "https://hooks.slack.com/services/T000/B000/XXXX", "channel": "#alerts"}},
		}},
	}},
	"tenants": {list: true, items: []namedItem{
		{"acme", example{set: map[string]any{"database": "main", "quota": 1000, "features": []string{"export", "sso"}, "api_key": "change-me"}}},
		{"globex", example{zh: "沒寫的欄位取 default 標籤的值，例如 quota 是 100。", only: []string{"database"}, set: map[string]any{"database": "main"}}},
	}},
}

var (
	tlsType      = reflect.TypeFor[TLS]()
	logType      = reflect.TypeFor[Log]()
	byteSizeType = reflect.TypeFor[ByteSize]()
)

type gen struct {
	fset      *token.FileSet
	pkgNames  map[string]bool // exported top-level names of the non-test files
	custom    *ast.File
	customSrc []byte

	out    strings.Builder
	used   map[string]bool         // docs keys written to example.yaml
	fullAt map[reflect.Type]string // where TLS was first written in full
	defs   map[string]omap
	errs   []string
}

func newGen(t *testing.T) *gen {
	g := &gen{
		fset:     token.NewFileSet(),
		pkgNames: map[string]bool{},
		used:     map[string]bool{},
		fullAt:   map[reflect.Type]string{},
		defs:     map[string]omap{},
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(g.fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					g.pkgNames[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.TypeSpec:
						g.pkgNames[s.Name.Name] = s.Name.IsExported()
					case *ast.ValueSpec:
						for _, n := range s.Names {
							g.pkgNames[n.Name] = n.IsExported()
						}
					}
				}
			}
		}
	}
	if g.customSrc, err = os.ReadFile(customFile); err != nil {
		t.Fatal(err)
	}
	if g.custom, err = parser.ParseFile(g.fset, customFile, g.customSrc, parser.ParseComments); err != nil {
		t.Fatal(err)
	}
	return g
}

func (g *gen) failf(format string, args ...any) {
	g.errs = append(g.errs, fmt.Sprintf(format, args...))
}

// builtin reports whether t is declared by the package rather than by the
// example, which decides whether schema.json covers it.
func (g *gen) builtin(t reflect.Type) bool {
	switch {
	case isNamed(t):
		t = namedElemType(t)
	case t.Kind() == reflect.Map:
		t = t.Elem()
	}
	return g.pkgNames[t.Name()]
}

// doc returns the docs entry of field f of struct t, keyed by the struct
// that declares it, so that squashed and shared structs are documented once.
func (g *gen) doc(t reflect.Type, f fieldInfo) (string, fieldDoc) {
	for _, i := range f.index[:len(f.index)-1] {
		if t = t.Field(i).Type; t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
	}
	key := t.Name() + "." + f.key
	d, ok := docs[key]
	if !ok {
		g.failf("docs has no entry %q", key)
	}
	return key, d
}

func fieldsOf(t reflect.Type) []fieldInfo { return getStructInfo(t, DefaultTagName).fields }

func defaults(t reflect.Type) reflect.Value {
	v := reflect.New(t).Elem()
	(&decoder{tagName: DefaultTagName}).applyDefaults("", v)
	return v
}

// plain converts a Go value to what a config file holds.
func plain(v reflect.Value) any {
	switch x := v.Interface().(type) {
	case time.Duration:
		// 5m and 1h rather than the 5m0s and 1h0m0s of String.
		s := x.String()
		if strings.HasSuffix(s, "m0s") {
			s = strings.TrimSuffix(s, "0s")
		}
		if strings.HasSuffix(s, "h0m") {
			s = strings.TrimSuffix(s, "0m")
		}
		return s
	case ByteSize:
		return x.String()
	case Secret:
		return x.Value()
	}
	switch v.Kind() {
	case reflect.Slice:
		if v.Len() == 0 {
			return []any{}
		}
	case reflect.Map:
		if v.Len() == 0 {
			return map[string]any{}
		}
	}
	return v.Interface()
}

func flow(v any) string {
	b, err := yaml.MarshalWithOptions(v, yaml.Flow(true))
	if err != nil {
		panic(err)
	}
	return strings.TrimSpace(string(b))
}

func (g *gen) line(indent, s string) { g.out.WriteString(indent + s + "\n") }

func (g *gen) comment(indent, text string) {
	for l := range strings.SplitSeq(text, "\n") {
		g.line(indent, strings.TrimRight("# "+l, " "))
	}
}

// yaml writes example.yaml.
func (g *gen) yaml() []byte {
	g.out.WriteString(`# yaml-language-server: $schema=./schema.json
#
# loadconf 設定範例：列出所有內建設定，最後附三個自訂設定的範例。
# 本檔由 config/example_gen_test.go 從 Go 型別產生，請勿手動修改；更新請執行 make generate。
#
# - 每個值都能通過驗證；沒特別寫的是預設值，空值代表不使用該功能。
# - 互斥的欄位只填其中一個，另一個留空並在註解說明。
# - schema.json 只描述內建設定。每個內建區段（log 除外）都可寫成單一設定，
#   或 config.Named 的多個實例：以名稱為鍵的對映，或每項帶 name 的清單。
#
# 載入本檔的 Go 程式：
#
#   cfg, err := config.Load[AppConfig](ctx,
#       config.From(config.File("example.yaml")),
#       config.WithStrict(), // 未知的鍵視為錯誤
#   )
#
`)
	g.source("AppConfig")

	root := reflect.TypeFor[AppConfig]()
	customs := false
	for _, f := range fieldsOf(root) {
		key, d := g.doc(root, f)
		g.used[key] = true
		if !g.builtin(f.typ) && !customs {
			customs = true
			g.out.WriteString("\n")
			g.line("", "# ════════════════════════════════════════════════════════════")
			g.line("", "# 自訂設定：以下是應用程式自己的區段，不在 schema.json 內。")
			g.line("", "# ════════════════════════════════════════════════════════════")
		}
		g.out.WriteString("\n")
		g.line("", "# ── "+f.key+"："+g.typeName(f.typ)+" ──")
		g.comment("", d.zh)
		if len(d.src) > 0 {
			g.line("", "#")
			g.source(d.src...)
		}
		ex := sectionExamples[f.key]
		switch {
		case isNamed(f.typ):
			g.line("", f.key+":")
			g.named(f.key, namedElemType(f.typ), "  ")
		case f.typ.Kind() == reflect.Struct && !isOpaque(f.typ):
			g.line("", f.key+":")
			g.fields(f.typ, defaults(f.typ), f.key, "", ex, "  ", "")
		default:
			g.value(f.key, d.ex, "")
		}
	}

	for key := range docs {
		if !g.used[key] {
			g.failf("example.yaml does not show %q (or docs has a stale entry)", key)
		}
	}
	return []byte(g.out.String())
}

var configQualified = regexp.MustCompile(`config\.(\w+)`)

// typeName prints a field type as the application spells it.
func (g *gen) typeName(t reflect.Type) string {
	s := strings.ReplaceAll(t.String(), "github.com/linzeyan/loadconf/config.", "config.")
	return configQualified.ReplaceAllStringFunc(s, func(m string) string {
		if g.pkgNames[m[len("config."):]] {
			return m
		}
		return m[len("config."):]
	})
}

func (g *gen) value(key string, v any, indent string) {
	es, ok := v.(entries)
	if !ok {
		g.line(indent, key+": "+flow(v))
		return
	}
	g.line(indent, key+":")
	for _, e := range es {
		g.comment(indent+"  ", e.zh)
		g.line(indent+"  ", e.key+": "+flow(e.val))
	}
}

// fields writes the fields of struct t at path. Values come from ex.set by
// key path below the example root (rel), then from the docs table, then
// from dv, which holds the defaults. skip names a key to leave out.
func (g *gen) fields(t reflect.Type, dv reflect.Value, path, rel string, ex example, indent, skip string) {
	for _, f := range fieldsOf(t) {
		// The skipped key is shown as the instance name.
		if f.key != skip && rel == "" && ex.only != nil && !slices.Contains(ex.only, f.key) {
			continue
		}
		key, d := g.doc(t, f)
		g.used[key] = true
		if f.key == skip {
			continue
		}
		fv, _ := fieldByIndex(dv, f.index, false)
		p, r := joinPath(path, f.key), joinPath(rel, f.key)
		g.comment(indent, d.zh)
		switch ft := f.typ; {
		case isNamed(ft):
			g.line(indent, f.key+":")
			g.named(p, namedElemType(ft), indent+"  ")
		case ft == tlsType && g.fullAt[ft] != "":
			// TLS repeats in nearly every section; after the first, only the
			// switch is shown.
			g.comment(indent, "其他欄位見 "+g.fullAt[ft]+"。")
			enabled, ok := ex.set[r+".enabled"]
			if !ok {
				enabled = false
			}
			g.line(indent, f.key+": "+flow(yaml.MapSlice{{Key: "enabled", Value: enabled}}))
		case ft.Kind() == reflect.Struct && !isOpaque(ft):
			if ft == tlsType {
				g.fullAt[ft] = p
			}
			g.line(indent, f.key+":")
			sub := ex
			sub.only = nil
			g.fields(ft, fv, p, r, sub, indent+"  ", "")
		default:
			v, ok := ex.set[r]
			if !ok {
				v = d.ex
			}
			if v == nil {
				v = plain(fv)
			}
			g.value(f.key, v, indent)
		}
	}
}

func (g *gen) named(path string, t reflect.Type, indent string) {
	nx, ok := namedExamples[path]
	if !ok {
		nx = namedExample{items: []namedItem{{name: "main"}}}
	}
	for _, it := range nx.items {
		if it.zh != "" {
			g.comment(indent, it.zh)
		}
		if nx.list {
			g.line(indent, "- name: "+it.name)
		} else {
			g.line(indent, it.name+":")
		}
		g.fields(t, defaults(t), joinPath(path, it.name), "", it.example, indent+"  ", "name")
	}
}

// source prints declarations of the custom file as comments: "T" for a type,
// "T.M" for a method. Identifiers of this package get "config." so the code
// reads as an application's.
func (g *gen) source(names ...string) {
	var b bytes.Buffer
	for _, name := range names {
		typ, method, _ := strings.Cut(name, ".")
		var decl ast.Decl
		var doc *ast.CommentGroup
		for _, d := range g.custom.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				for _, s := range d.Specs {
					if ts, ok := s.(*ast.TypeSpec); ok && method == "" && ts.Name.Name == typ {
						decl, doc = d, d.Doc
					}
				}
			case *ast.FuncDecl:
				if d.Recv != nil && method == d.Name.Name && recvName(d) == typ {
					decl, doc = d, d.Doc
				}
			}
		}
		if decl == nil {
			g.failf("%s declares no %s", customFile, name)
			continue
		}
		b.WriteString(g.qualify(decl, doc))
		b.WriteString("\n\n")
	}
	out, err := format.Source(b.Bytes())
	if err != nil {
		g.failf("format %v: %v", names, err)
		return
	}
	for l := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		g.line("", strings.TrimRight("#   "+strings.ReplaceAll(l, "\t", "    "), " "))
	}
}

func recvName(d *ast.FuncDecl) string {
	t := d.Recv.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
	}
	id, _ := t.(*ast.Ident)
	return id.Name
}

func (g *gen) qualify(decl ast.Decl, doc *ast.CommentGroup) string {
	start := decl.Pos()
	if doc != nil {
		start = doc.Pos()
	}
	skip := map[*ast.Ident]bool{}
	var at []int
	ast.Inspect(decl, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			skip[x.Sel] = true
		case *ast.Field:
			for _, id := range x.Names {
				skip[id] = true
			}
		case *ast.KeyValueExpr:
			if id, ok := x.Key.(*ast.Ident); ok {
				skip[id] = true
			}
		case *ast.FuncDecl:
			skip[x.Name] = true
		case *ast.TypeSpec:
			skip[x.Name] = true
		case *ast.Ident:
			if !skip[x] && g.pkgNames[x.Name] {
				at = append(at, g.fset.Position(x.Pos()).Offset)
			}
		}
		return true
	})
	from, to := g.fset.Position(start).Offset, g.fset.Position(decl.End()).Offset
	var b strings.Builder
	for _, off := range at {
		b.Write(g.customSrc[from:off])
		b.WriteString("config.")
		from = off
	}
	b.Write(g.customSrc[from:to])
	return b.String()
}

// omap is a JSON object that keeps its key order.
type omap []kv

type kv struct {
	k string
	v any
}

func (m omap) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, e := range m {
		if i > 0 {
			b.WriteByte(',')
		}
		for _, v := range []any{e.k, e.v} {
			enc := json.NewEncoder(&b)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(v); err != nil {
				return nil, err
			}
			b.Truncate(b.Len() - 1) // Encode appends a newline
			if v == any(e.k) {
				b.WriteByte(':')
			}
		}
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// schema writes schema.json.
func (g *gen) schema() []byte {
	root := reflect.TypeFor[AppConfig]()
	var props omap
	for _, f := range fieldsOf(root) {
		if !g.builtin(f.typ) {
			continue
		}
		_, d := g.doc(root, f)
		t := f.typ
		if isNamed(t) {
			t = namedElemType(t)
		}
		s := omap{{"description", d.zh}}
		if t == logType {
			s = append(s, g.ref(t)...)
		} else {
			s = append(s, kv{"anyOf", []any{g.ref(t), g.named0(t)}})
		}
		props = append(props, kv{f.key, s})
	}
	var defs omap
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range g.defs {
			if !yield(k) {
				return
			}
		}
	}) {
		defs = append(defs, kv{name, g.defs[name]})
	}
	doc := omap{
		{"$schema", "https://json-schema.org/draft/2020-12/schema"},
		{"$id", schemaID},
		{"title", "loadconf"},
		{"description", "loadconf 內建設定（由 config/example_gen_test.go 產生）。區段名稱是慣例，實際以應用程式的 struct 標籤為準；未列出的頂層鍵不檢查。"},
		{"type", "object"},
		{"properties", props},
		{"$defs", defs},
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		g.failf("encode schema: %v", err)
	}
	return b.Bytes()
}

// ref points to the definition of struct t. unevaluatedProperties sits at
// the use site rather than in the definition, so a Named item can add name.
func (g *gen) ref(t reflect.Type) omap {
	if _, ok := g.defs[t.Name()]; !ok {
		g.defs[t.Name()] = nil // reserve against recursion
		g.defs[t.Name()] = g.def(t)
	}
	return omap{{"$ref", "#/$defs/" + t.Name()}, {"unevaluatedProperties", false}}
}

// named0 is the Named form of t: a mapping by name or a list of items with a
// name.
func (g *gen) named0(t reflect.Type) omap {
	item := func(list bool) omap {
		m := omap{
			{"$ref", "#/$defs/" + t.Name()},
			{"properties", omap{{"name", omap{{"type", "string"}}}}},
		}
		if list {
			m = append(m, kv{"required", []string{"name"}})
		}
		return append(m, kv{"unevaluatedProperties", false})
	}
	g.ref(t)
	return omap{{"anyOf", []any{
		omap{{"type", "object"}, {"additionalProperties", item(false)}},
		omap{{"type", "array"}, {"items", item(true)}},
	}}}
}

func (g *gen) def(t reflect.Type) omap {
	dv := defaults(t)
	var props omap
	for _, f := range fieldsOf(t) {
		_, d := g.doc(t, f)
		fv, _ := fieldByIndex(dv, f.index, false)
		props = append(props, kv{f.key, g.prop(f.typ, d, fv)})
	}
	return omap{{"type", "object"}, {"properties", props}}
}

func (g *gen) prop(t reflect.Type, d fieldDoc, fv reflect.Value) omap {
	s := omap{{"description", d.zh}}
	scalar := true
	switch {
	case t == durationType:
		g.defs["Duration"] = omap{
			{"description", "時間長度，例如 500ms、5s、1m30s、1h；數字只接受 0（其他數字的單位不明）。"},
			{"anyOf", []any{
				omap{{"type", "string"}, {"pattern", `^(|0|-?((\d+(\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h))+)$`}},
				omap{{"const", 0}},
			}},
		}
		s = append(s, kv{"$ref", "#/$defs/Duration"})
	case t == byteSizeType:
		g.defs["ByteSize"] = omap{
			{"description", "位元組數，或含單位的字串：B、KB/MB/GB/TB（1000 進位）、KiB/MiB/GiB/TiB（1024 進位），例如 64MiB。"},
			{"anyOf", []any{
				omap{{"type", "integer"}, {"minimum", 0}},
				omap{{"type", "string"}, {"pattern", `^\s*(\d+\.?\d*|\.\d+)\s*([bB]|[kKmMgGtT]([iI]?[bB])?)?\s*$`}},
			}},
		}
		s = append(s, kv{"$ref", "#/$defs/ByteSize"})
	case t == secretType:
		s = append(s, kv{"type", "string"})
	case isNamed(t):
		scalar = false
		s = append(s, g.named0(namedElemType(t))...)
	case t.Kind() == reflect.Struct && !isOpaque(t):
		scalar = false
		s = append(s, g.ref(t)...)
	case t.Kind() == reflect.String:
		s = append(s, kv{"type", "string"})
	case t.Kind() == reflect.Bool:
		s = append(s, kv{"type", "boolean"})
	case t.Kind() >= reflect.Int && t.Kind() <= reflect.Int64:
		s = append(s, kv{"type", "integer"})
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.String:
		// A comma-separated string decodes into a slice as well.
		scalar = false
		s = append(s, kv{"type", []string{"array", "string"}}, kv{"items", omap{{"type", "string"}}})
	case t.Kind() == reflect.Map && t.Key().Kind() == reflect.String && t.Elem().Kind() == reflect.String:
		scalar = false
		s = append(s, kv{"type", "object"}, kv{"additionalProperties", omap{{"type", []string{"string", "number", "boolean"}}}})
	case t.Kind() == reflect.Map && t.Key().Kind() == reflect.String && t.Elem().Kind() == reflect.Interface:
		scalar = false
		s = append(s, kv{"type", "object"})
	default:
		g.failf("no schema for type %s", t)
	}
	if len(d.enum) > 0 {
		s = append(s, kv{"enum", d.enum})
	}
	if scalar && !fv.IsZero() {
		s = append(s, kv{"default", plain(fv)})
	}
	return s
}

func TestExampleFiles(t *testing.T) {
	g := newGen(t)
	files := map[string][]byte{exampleFile: g.yaml(), schemaFile: g.schema()}
	if len(g.errs) > 0 {
		t.Fatal(strings.Join(g.errs, "\n"))
	}
	for name, want := range files {
		if *updateExample {
			if err := os.WriteFile(name, want, 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if got, err := os.ReadFile(name); err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s is stale: run make generate", name)
		}
	}
}

// The example must be a config that loads, not just one that parses: strict
// mode rejects keys the structs lack and every Validate hook runs.
func TestExampleLoads(t *testing.T) {
	ctx := context.Background()
	cfg, err := Load[AppConfig](ctx, From(File(exampleFile)), WithStrict(), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]RateLimit{
		"api":    {100, time.Second},
		"search": {20, 30 * time.Second},
		"login":  {5, time.Minute},
	} {
		if got := cfg.RateLimits[name]; got != want {
			t.Errorf("rate_limits.%s = %+v, want %+v", name, got, want)
		}
	}
	if acme := cfg.Tenants.MustGet("acme"); acme.Name != "acme" || acme.Quota != 1000 {
		t.Errorf("tenants.acme = %+v", acme)
	}
	if globex := cfg.Tenants.MustGet("globex"); globex.Quota != 100 {
		t.Errorf("tenants.globex.quota = %d, want the default 100", globex.Quota)
	}
	if cfg.Server.MaxBodySize != 8*MiB || cfg.Log.Outputs.Len() != 8 {
		t.Errorf("server.max_body_size = %v, log outputs = %d", cfg.Server.MaxBodySize, cfg.Log.Outputs.Len())
	}

	// The cross-section check the tenants section describes really runs.
	_, err = Load[AppConfig](ctx, From(File(exampleFile), Bytes("yaml", []byte("tenants: {acme: {database: billing}}"))), WithLogger(nil))
	if err == nil || !strings.Contains(err.Error(), `no mysql instance "billing"`) {
		t.Errorf("err = %v, want the tenant database check", err)
	}
}
