module github.com/linzeyan/loadconf/logger/logger_zap

go 1.26.0

require (
	github.com/linzeyan/loadconf/config v0.1.0
	github.com/linzeyan/loadconf/logger v0.1.0
	go.uber.org/zap v1.28.0
)

require (
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

replace github.com/linzeyan/loadconf/config => ../../config

replace github.com/linzeyan/loadconf/logger => ../../logger
