module github.com/linzeyan/loadconf/connector/conn_gorm_mssql

go 1.26.0

require (
	github.com/linzeyan/loadconf/config v0.1.0
	github.com/linzeyan/loadconf/connector/conn_mssql v0.1.0
	github.com/microsoft/go-mssqldb v1.11.2
	gorm.io/driver/sqlserver v1.6.4
	gorm.io/gorm v1.31.2
)

require (
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/linzeyan/loadconf/connector/conn_gorm_log v0.1.0
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config

replace github.com/linzeyan/loadconf/connector/conn_gorm_log => ../../connector/conn_gorm_log

replace github.com/linzeyan/loadconf/connector/conn_mssql => ../../connector/conn_mssql
