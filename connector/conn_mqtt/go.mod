module github.com/linzeyan/loadconf/connector/conn_mqtt

go 1.26.0

require (
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/linzeyan/loadconf/config v0.1.0
)

require (
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config
