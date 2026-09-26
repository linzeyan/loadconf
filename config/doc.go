// Package config loads application configuration from layered sources into
// a user-defined struct.
//
// Sources (files, environment variables, etcd, ...) are loaded in order and
// deep-merged, so later sources override earlier ones. The merged tree is then
// decoded into the target struct, defaults are applied and the result is
// validated:
//
//	type AppConf struct {
//		Name  string                     `config:"name" validate:"required"`
//		Port  int                        `config:"port" env:"PORT" default:"8080"`
//		MySQL config.Named[config.MySQL] `config:"mysql"`
//		Redis config.Named[config.Redis] `config:"redis"`
//	}
//
//	cfg, err := config.Load[AppConf](ctx, config.From(
//		config.Profile("app"), // ./configs/app.yaml + ./configs/app.{ENV}.yaml
//		config.Env("APP"),     // APP_MYSQL__ORDERS__DSN=... overrides mysql.orders.dsn
//	))
//	orders, ok := cfg.MySQL.Get("orders")
//
// # Struct tags
//
//   - config:"key"       key name in the source tree; defaults to the snake_case
//     field name, with product names kept whole (MySQL is mysql, ClickHouse is
//     clickhouse). config:"-" skips the field and config:",squash" (or ",inline")
//     flattens a nested struct into its parent. Embedded structs without a key
//     are flattened automatically.
//   - default:"value"    value applied before decoding; parsed like any string
//     source value (durations as "5s", slices as "a,b").
//   - env:"NAME[,ALT]"   explicit environment variable that overrides the field
//     after every source has been merged.
//   - env-prefix:"P_"    prefix prepended to the env tags of nested fields.
//   - validate:"rules"   github.com/go-playground/validator/v10 rules.
//
// Types may also implement [Defaulter], [Validator] or [Unmarshaler] to hook
// into the corresponding stage.
//
// # Multiple instances
//
// [Named] holds several named instances of the same type, e.g. several MySQL
// databases. In a file they can be written either as a mapping keyed by name
// or as a list of items carrying a "name" key; names are case-insensitive.
//
// # Hot reload
//
// [Loader.Watch] re-loads the configuration whenever a watchable source (files,
// etcd) changes and notifies [Loader.OnChange] subscribers.
package config
