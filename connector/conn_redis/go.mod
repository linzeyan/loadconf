module github.com/linzeyan/loadconf/connector/conn_redis

go 1.26.0

require (
	github.com/linzeyan/loadconf/config v0.1.0
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config
