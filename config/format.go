package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/goccy/go-yaml"
	"github.com/pelletier/go-toml/v2"
)

// ParseFunc parses a document into a raw tree whose root is a mapping.
type ParseFunc func(data []byte) (map[string]any, error)

var formats = struct {
	sync.RWMutex
	byName map[string]ParseFunc
	byExt  map[string]string
}{
	byName: map[string]ParseFunc{},
	byExt:  map[string]string{},
}

func init() {
	RegisterFormat("json", parseJSON, ".json")
	RegisterFormat("yaml", parseYAML, ".yaml", ".yml")
	RegisterFormat("toml", parseTOML, ".toml")
}

// RegisterFormat registers (or replaces) a document format and the file
// extensions that select it, e.g. RegisterFormat("hcl", parseHCL, ".hcl").
func RegisterFormat(name string, parse ParseFunc, exts ...string) {
	formats.Lock()
	defer formats.Unlock()
	name = strings.ToLower(name)
	formats.byName[name] = parse
	for _, ext := range exts {
		formats.byExt[strings.ToLower(ext)] = name
	}
}

// Parse parses data in the named format ("json", "yaml", "toml" or a
// registered one).
func Parse(format string, data []byte) (map[string]any, error) {
	formats.RLock()
	parse, ok := formats.byName[strings.ToLower(format)]
	formats.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown config format %q", format)
	}
	m, err := parse(data)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", format, err)
	}
	if m == nil {
		return map[string]any{}, nil
	}
	out, err := sanitize(m)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", format, err)
	}
	return out.(map[string]any), nil
}

// FormatOf returns the format registered for the extension of path.
func FormatOf(path string) (string, error) {
	ext := strings.ToLower(filepath.Ext(path))
	formats.RLock()
	name, ok := formats.byExt[ext]
	formats.RUnlock()
	if !ok {
		return "", fmt.Errorf("cannot infer config format from extension %q of %s", ext, path)
	}
	return name, nil
}

// knownExts lists the registered extensions, built-in ones first.
func knownExts() []string {
	builtin := []string{".yaml", ".yml", ".json", ".toml"}
	formats.RLock()
	defer formats.RUnlock()
	var extra []string
	for ext := range formats.byExt {
		if !slices.Contains(builtin, ext) {
			extra = append(extra, ext)
		}
	}
	slices.Sort(extra)
	return append(builtin, extra...)
}

func parseJSON(data []byte) (map[string]any, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the top-level object")
	}
	return m, nil
}

func parseYAML(data []byte) (map[string]any, error) {
	var m map[string]any
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func parseTOML(data []byte) (map[string]any, error) {
	var m map[string]any
	if err := toml.Unmarshal(data, &m); err != nil {
		// Error() is the bare message; the position and the lines around it
		// are only in DecodeError. Lead with [line:column] like YAML errors.
		var de *toml.DecodeError
		if errors.As(err, &de) {
			row, col := de.Position()
			return nil, fmt.Errorf("[%d:%d] %s\n%s", row, col, strings.TrimPrefix(de.Error(), "toml: "), de.String())
		}
		return nil, err
	}
	return m, nil
}
