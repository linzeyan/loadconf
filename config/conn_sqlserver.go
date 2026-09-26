package config

import (
	"errors"
	"time"
)

// SQLServer configures a Microsoft SQL Server or Azure SQL connection
// (go-mssqldb).
//
// Either DSN (sqlserver:// URL, ADO or ODBC form) or Host is required. Fields
// set next to a DSN override the corresponding DSN parts.
type SQLServer struct {
	DSN  Secret `config:"dsn"`
	Host string `config:"host"`
	Port int    `config:"port" default:"1433"`
	// Instance selects a named instance through the SQL Server Browser; Port
	// is ignored then.
	Instance string `config:"instance"`
	User     string `config:"user"`
	Password Secret `config:"password"`
	Database string `config:"database"`
	// Params are go-mssqldb connection parameters, e.g. "app name", "dial
	// timeout" (in seconds), "packet size", "log" or encrypt: disable (no
	// TLS), false (encrypt the login only), true (encrypt everything) or
	// strict (TDS 8). Enabling TLS implies encrypt true unless set.
	Params map[string]string `config:"params"`

	PingTimeout time.Duration `config:"ping_timeout" default:"3s"`

	// TLS configures certificate verification; CA, server name and client
	// certificates apply when encryption is on.
	TLS     TLS `config:"tls"`
	SQLPool `config:",squash"`
}

func (s SQLServer) Validate() error {
	var errs []error
	if s.DSN.Value() == "" && s.Host == "" {
		errs = append(errs, errors.New("either dsn or host is required"))
	}
	return errors.Join(append(errs, s.SQLPool.Validate())...)
}
