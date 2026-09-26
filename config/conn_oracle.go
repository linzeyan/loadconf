package config

import (
	"errors"
	"time"
)

// Oracle configures an Oracle Database connection (go-ora, pure Go).
//
// One of DSN (oracle://user:pass@host:port/service?options), ConnectString
// (EZConnect or a TNS descriptor) or Host is required. Fields set next to a
// DSN override the corresponding DSN parts.
type Oracle struct {
	DSN  Secret `config:"dsn"`
	Host string `config:"host"`
	Port int    `config:"port" default:"1521"`
	// Service is the service name; SID the legacy system identifier. Set one.
	Service string `config:"service"`
	SID     string `config:"sid"`
	// ConnectString is an EZConnect string or a full TNS descriptor such as
	// (DESCRIPTION=(ADDRESS_LIST=...)(CONNECT_DATA=...)), used instead of
	// Host, Port, Service and SID, e.g. for RAC or failover setups.
	ConnectString string `config:"connect_string"`
	User          string `config:"user"`
	Password      Secret `config:"password"`
	// Params are go-ora URL options, e.g. "CONNECTION TIMEOUT" (seconds),
	// "PREFETCH_ROWS", "LOB FETCH", "TRACE FILE" or "WALLET".
	Params map[string]string `config:"params"`

	PingTimeout time.Duration `config:"ping_timeout" default:"3s"`

	// TLS enables TCPS. go-ora verifies against a wallet (Params["WALLET"])
	// or the system roots; InsecureSkipVerify disables verification.
	TLS     TLS `config:"tls"`
	SQLPool `config:",squash"`
}

func (o Oracle) Validate() error {
	var errs []error
	if o.DSN.Value() == "" && o.ConnectString == "" && o.Host == "" {
		errs = append(errs, errors.New("one of dsn, connect_string or host is required"))
	}
	if o.Service != "" && o.SID != "" {
		errs = append(errs, errors.New("service and sid are mutually exclusive"))
	}
	return errors.Join(append(errs, o.SQLPool.Validate())...)
}
