#!/usr/bin/env bash
# Adds to each go.mod a replace for every require of another module of this
# repository, pointing at that module's directory. Every module then builds,
# tests and runs go mod tidy against the working tree, also before a version
# is tagged. Consumers ignore replace directives, so the requires still name
# the released versions they get. Run it after adding a module or a require
# between modules, then go mod tidy; it is idempotent.
set -u
cd "$(dirname "$0")/.." || exit 1

prefix=github.com/linzeyan/loadconf
for gomod in $(git ls-files --cached --others --exclude-standard '*/go.mod'); do
	dir=$(dirname "$gomod")
	up=$(echo "$dir/" | sed 's|[^/]*/|../|g')
	# Requires carry a version; replace lines ("path => dir") do not.
	for dep in $(awk -v p="$prefix/" '{for (i = 1; i < NF; i++) if (index($i, p) == 1 && $(i + 1) ~ /^v/) print $i}' "$gomod" | sort -u); do
		(cd "$dir" && go mod edit -replace="$dep=$up${dep#"$prefix"/}") || exit 1
	done
done
