package config

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

// watchFiles calls changed when the content of any of the files changes. It
// watches the parent directories rather than the files themselves so that
// editors replacing files via rename and Kubernetes ConfigMap symlink swaps
// are noticed; content hashes filter out unrelated events.
func watchFiles(ctx context.Context, paths []string, changed func()) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()

	type watchedFile struct {
		path, dir string
		hash      [sha256.Size]byte
	}
	files := make([]*watchedFile, 0, len(paths))
	dirs := map[string]bool{}
	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return err
		}
		f := &watchedFile{path: abs, dir: filepath.Dir(abs), hash: hashFile(abs)}
		files = append(files, f)
		if !dirs[f.dir] {
			// A missing directory, e.g. that of an Optional overlay, has nothing to
			// report and must not keep the other files from being watched.
			// ponytail: a directory created later is not picked up until Watch
			// restarts; watch the nearest existing parent if that matters.
			if err := w.Add(f.dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("watch %s: %w", f.dir, err)
			}
			dirs[f.dir] = true
		}
	}
	// An edit made after the config was loaded but before the watches above
	// were set up raised no event; one check now that later edits are seen
	// closes that gap. Spurious reloads are dropped by the loader.
	changed()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.Events:
			if !ok {
				return nil
			}
			dir := filepath.Dir(ev.Name)
			hit := false
			for _, f := range files {
				if f.dir != dir {
					continue
				}
				if h := hashFile(f.path); h != f.hash {
					f.hash = h
					hit = true
				}
			}
			if hit {
				changed()
			}
		case err, ok := <-w.Errors:
			if !ok {
				return nil
			}
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				changed() // events were lost; let the loader re-check
				continue
			}
			return err
		}
	}
}

// hashFile returns the content hash, or the zero hash if the file is missing.
func hashFile(path string) [sha256.Size]byte {
	data, err := os.ReadFile(path)
	if err != nil {
		return [sha256.Size]byte{}
	}
	return sha256.Sum256(data)
}
