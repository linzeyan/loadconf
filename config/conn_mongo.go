package config

import (
	"errors"
	"time"
)

// Mongo configures a MongoDB client. Either URI or Hosts is required; fields
// set next to a URI override the URI.
type Mongo struct {
	// URI is a mongodb:// or mongodb+srv:// connection string.
	URI      Secret   `config:"uri"`
	Hosts    []string `config:"hosts"`
	Database string   `config:"database"`

	Username      string `config:"username"`
	Password      Secret `config:"password"`
	AuthSource    string `config:"auth_source"`
	AuthMechanism string `config:"auth_mechanism"`
	// Params are connection string options, e.g. replicaSet, appName,
	// readPreference, maxPoolSize, directConnection, compressors or timeoutMS.
	// They override the same options of URI.
	Params map[string]string `config:"params"`

	PingTimeout time.Duration `config:"ping_timeout" default:"3s"`
	TLS         TLS           `config:"tls"`
}

func (m Mongo) Validate() error {
	if m.URI.Value() == "" && len(m.Hosts) == 0 {
		return errors.New("either uri or hosts is required")
	}
	return nil
}
