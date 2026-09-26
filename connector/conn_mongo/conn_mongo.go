// Package conn_mongo opens MongoDB clients (mongo-driver v2) from
// [config.Mongo].
//
//	db, err := conn_mongo.OpenDatabase(ctx, cfg.Mongo.MustGet("main"))
//	defer db.Client().Disconnect(context.Background())
//
// The driver's structured logs go to slog.Default() at
// options.LogLevelInfo; [WithLogger] picks another logger or level.
package conn_mongo

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/linzeyan/loadconf/config"
)

// Option adjusts the client options after they are built from the config,
// e.g. to add monitors or a BSON registry.
type Option func(*options.ClientOptions)

// ClientOptions converts cfg into driver options: the URI, or Hosts, with
// cfg.Params as connection string options, then the typed fields.
func ClientOptions(cfg config.Mongo) (*options.ClientOptions, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	uri, err := connString(cfg)
	if err != nil {
		return nil, err
	}
	o := options.Client().ApplyURI(uri)
	if len(cfg.Hosts) > 0 {
		o.SetHosts(cfg.Hosts)
	}

	if cfg.Username != "" || cfg.Password.Value() != "" || cfg.AuthSource != "" || cfg.AuthMechanism != "" {
		cred := options.Credential{}
		if o.Auth != nil {
			cred = *o.Auth
		}
		if cfg.Username != "" {
			cred.Username = cfg.Username
		}
		if cfg.Password.Value() != "" {
			cred.Password, cred.PasswordSet = cfg.Password.Value(), true
		}
		if cfg.AuthSource != "" {
			cred.AuthSource = cfg.AuthSource
		}
		if cfg.AuthMechanism != "" {
			cred.AuthMechanism = cfg.AuthMechanism
		}
		o.SetAuth(cred)
	}
	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, err
		}
		o.SetTLSConfig(tlsCfg)
	}
	WithLogger(nil, options.LogLevelInfo)(o)
	if err := o.Validate(); err != nil {
		return nil, err
	}
	return o, nil
}

// connString returns cfg.URI, or a URI naming cfg.Hosts, with cfg.Params
// replacing the URI's options of the same name. Option names are
// case-insensitive, so a differently cased duplicate is dropped too.
func connString(cfg config.Mongo) (string, error) {
	uri := cfg.URI.Value()
	if uri == "" {
		// The driver unescapes hosts and ends them at the first '/', so a
		// Unix socket path must be escaped. SetHosts sets them verbatim later.
		hosts := make([]string, len(cfg.Hosts))
		for i, h := range cfg.Hosts {
			hosts[i] = url.QueryEscape(h)
		}
		uri = "mongodb://" + strings.Join(hosts, ",") + "/"
	}
	if len(cfg.Params) == 0 {
		return uri, nil
	}
	names := slices.Sorted(maps.Keys(cfg.Params))
	for i, k := range names {
		// Either spelling could win, so neither does.
		if j := slices.IndexFunc(names[:i], func(n string) bool { return strings.EqualFold(n, k) }); j >= 0 {
			return "", fmt.Errorf("mongo params %q and %q name the same option", names[j], k)
		}
	}
	// The driver separates options with ';' as well as '&'. The URI's
	// options are kept verbatim; the driver reports any it cannot parse
	// without printing the URI, which may hold a password.
	base, query, _ := strings.Cut(uri, "?")
	var opts []string
	for _, pair := range strings.FieldsFunc(query, func(r rune) bool { return r == '&' || r == ';' }) {
		name, _, _ := strings.Cut(pair, "=")
		if name, err := url.QueryUnescape(name); err == nil && slices.ContainsFunc(names, func(n string) bool { return strings.EqualFold(n, name) }) {
			continue // replaced by the param
		}
		opts = append(opts, pair)
	}
	for _, k := range names {
		opts = append(opts, url.QueryEscape(k)+"="+url.QueryEscape(cfg.Params[k]))
	}
	// Options must follow a slash: mongodb://host/?options.
	if _, rest, _ := strings.Cut(base, "://"); !strings.Contains(rest, "/") {
		base += "/"
	}
	return base + "?" + strings.Join(opts, "&"), nil
}

// New creates a client without waiting for the servers: the driver connects
// in the background and reports failures on first use.
func New(cfg config.Mongo, opts ...Option) (*mongo.Client, error) {
	client, _, err := newClient(cfg, opts)
	return client, err
}

// Open is [New] plus a ping to the primary (or the configured read
// preference) within cfg.PingTimeout. The client is disconnected if the ping
// fails.
func Open(ctx context.Context, cfg config.Mongo, opts ...Option) (*mongo.Client, error) {
	client, o, err := newClient(cfg, opts)
	if err != nil {
		return nil, err
	}

	pingCtx := ctx
	if cfg.PingTimeout > 0 {
		var cancel context.CancelFunc
		pingCtx, cancel = context.WithTimeout(ctx, cfg.PingTimeout)
		defer cancel()
	}
	if err := client.Ping(pingCtx, o.ReadPreference); err != nil {
		_ = client.Disconnect(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	return client, nil
}

func newClient(cfg config.Mongo, opts []Option) (*mongo.Client, *options.ClientOptions, error) {
	o, err := ClientOptions(cfg)
	if err != nil {
		return nil, nil, err
	}
	for _, opt := range opts {
		opt(o)
	}
	client, err := mongo.Connect(o)
	if err != nil {
		return nil, nil, err
	}
	return client, o, nil
}

// OpenDatabase opens a client and returns cfg.Database on it. Disconnect via
// db.Client().Disconnect.
func OpenDatabase(ctx context.Context, cfg config.Mongo, opts ...Option) (*mongo.Database, error) {
	if cfg.Database == "" {
		return nil, errors.New("mongo: database is not configured")
	}
	client, err := Open(ctx, cfg, opts...)
	if err != nil {
		return nil, err
	}
	return client.Database(cfg.Database), nil
}
