module github.com/linzeyan/loadconf/connector/conn_cassandra

go 1.26.0

require (
	github.com/apache/cassandra-gocql-driver/v2 v2.1.2
	github.com/linzeyan/loadconf/config v0.1.0
)

require (
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.8 // indirect
	golang.org/x/sys v0.48.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config
