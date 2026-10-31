module github.com/linzeyan/loadconf/connector/conn_kafka_franz

go 1.26.0

require github.com/linzeyan/loadconf/config v0.1.0

require (
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.30 // indirect
	github.com/twmb/franz-go v1.22.0
	github.com/twmb/franz-go/pkg/kmsg v1.14.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config
