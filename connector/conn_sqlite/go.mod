module github.com/linzeyan/loadconf/connector/conn_sqlite

go 1.26.0

require (
	github.com/linzeyan/loadconf/config v0.1.0
	github.com/ncruces/go-sqlite3 v0.35.6
)

require (
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/ncruces/go-sqlite3-wasm/v6 v6.3.35304 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config
