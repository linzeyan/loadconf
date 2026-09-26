package config

import (
	"errors"
	"fmt"
	"time"
)

// Redis modes.
const (
	RedisSingle   = "single"
	RedisCluster  = "cluster"
	RedisSentinel = "sentinel"
)

// Redis configures a standalone, cluster or sentinel (failover) client.
//
// Options sets any other go-redis setting by the snake_case name of its
// redis.UniversalOptions field, e.g. pool_size, read_timeout, client_name,
// protocol or route_by_latency; see [DecodeOptions]. Unset settings keep the
// go-redis defaults.
type Redis struct {
	// Mode is single, cluster or sentinel. When empty it is inferred:
	// sentinel if MasterName is set, cluster if several Addrs, else single.
	Mode string `config:"mode"`
	// Addrs are host:port pairs of the server, the cluster seed nodes or the
	// sentinels.
	Addrs      []string `config:"addrs"`
	MasterName string   `config:"master_name"`
	Username   string   `config:"username"`
	Password   Secret   `config:"password"`
	DB         int      `config:"db"`

	SentinelUsername string `config:"sentinel_username"`
	SentinelPassword Secret `config:"sentinel_password"`

	PingTimeout time.Duration  `config:"ping_timeout" default:"3s"`
	TLS         TLS            `config:"tls"`
	Options     map[string]any `config:"options"`
}

// ResolvedMode returns Mode, or the inferred mode when Mode is empty.
func (r Redis) ResolvedMode() string {
	switch {
	case r.Mode != "":
		return r.Mode
	case r.MasterName != "":
		return RedisSentinel
	case len(r.Addrs) > 1:
		return RedisCluster
	default:
		return RedisSingle
	}
}

func (r Redis) Validate() error {
	if len(r.Addrs) == 0 {
		return errors.New("addrs is required")
	}
	switch r.ResolvedMode() {
	case RedisSingle:
		if len(r.Addrs) > 1 {
			return errors.New("single mode takes exactly one address")
		}
	case RedisCluster:
		if r.DB != 0 {
			return errors.New("cluster mode supports db 0 only")
		}
	case RedisSentinel:
		if r.MasterName == "" {
			return errors.New("master_name is required in sentinel mode")
		}
	default:
		return fmt.Errorf("unsupported mode %q (want single, cluster or sentinel)", r.Mode)
	}
	return nil
}
