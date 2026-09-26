package config_test

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/linzeyan/loadconf/config"
)

func Example() {
	type AppConf struct {
		Name  string                     `config:"name" validate:"required"`
		Port  int                        `config:"port" default:"8080"`
		MySQL config.Named[config.MySQL] `config:"mysql"`
		Redis config.Named[config.Redis] `config:"redis"`
	}

	doc := `
name: order-service
mysql:
  - name: orders
    host: db1
    database: orders
redis:
  cache:
    addrs: ["127.0.0.1:6379"]
`
	os.Setenv("EXAMPLE_MYSQL__ORDERS__PASSWORD", "s3cret")
	defer os.Unsetenv("EXAMPLE_MYSQL__ORDERS__PASSWORD")

	cfg, err := config.Load[AppConf](context.Background(),
		config.WithLogger(nil),
		config.From(
			config.Bytes("yaml", []byte(doc)),
			config.Env("EXAMPLE"),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := cfg.MySQL.MustGet("orders")
	fmt.Println(cfg.Name, cfg.Port)
	fmt.Println(db.Host, db.Port, db.PingTimeout)
	fmt.Println(db.Password, db.Password.Value())
	fmt.Println(cfg.Redis.Names())
	// Output:
	// order-service 8080
	// db1 3306 3s
	// ****** s3cret
	// [cache]
}

func ExampleLoader_Watch() {
	type AppConf struct {
		Redis config.Named[config.Redis] `config:"redis"`
	}

	l := config.New[AppConf](config.From(config.Profile("app"), config.Env("APP")))
	cfg, err := l.Load(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	_ = cfg // build connections from cfg

	l.OnChange(func(old, next *AppConf) {
		added, removed, changed := old.Redis.Diff(next.Redis)
		log.Printf("redis instances added=%v removed=%v changed=%v", added, removed, changed)
		// Rebuild only the affected connections here.
	})
	l.OnError(func(err error) { log.Printf("config reload rejected: %v", err) })

	go func() {
		if err := l.Watch(context.Background()); err != nil {
			log.Printf("config watch stopped: %v", err)
		}
	}()
}
