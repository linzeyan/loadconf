module github.com/linzeyan/loadconf/connector/conn_gorm_sqlite

go 1.26.0

require (
	github.com/linzeyan/loadconf/config v0.1.0
	github.com/linzeyan/loadconf/connector/conn_gorm_log v0.1.0
	github.com/linzeyan/loadconf/connector/conn_sqlite v0.1.0
	github.com/ncruces/go-sqlite3 v0.35.6
	github.com/ncruces/go-sqlite3/gormlite v0.34.0
	gorm.io/gorm v1.31.2
)

require (
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/ncruces/go-sqlite3-wasm/v6 v6.3.35304 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config

replace github.com/linzeyan/loadconf/connector/conn_gorm_log => ../../connector/conn_gorm_log

replace github.com/linzeyan/loadconf/connector/conn_sqlite => ../../connector/conn_sqlite
