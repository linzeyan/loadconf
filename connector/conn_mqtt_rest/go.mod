module github.com/linzeyan/loadconf/connector/conn_mqtt_rest

go 1.26.0

require github.com/linzeyan/loadconf/config v0.1.0

require (
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config
