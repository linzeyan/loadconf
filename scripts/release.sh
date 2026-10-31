#!/usr/bin/env bash
# Releases every module at one version: points the requires between the
# modules at VERSION, commits, and tags each module <dir>/VERSION, which is
# how Go finds the version of a module in a subdirectory. One version for all
# modules means a connector's go.mod always names a config version tagged in
# the same commit. Pushing is left to the caller; the command is printed.
set -u
cd "$(dirname "$0")/.." || exit 1

version=${1:-}
if ! [[ $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
	echo "usage: $0 vX.Y.Z" >&2
	exit 2
fi
# Tags name module directories from the repository root, so this directory
# must be the root of github.com/linzeyan/loadconf.
if [ "$(git rev-parse --show-toplevel)" != "$(pwd -P)" ]; then
	echo "run from the root of the repository: tags like config/$version would not match the module directories" >&2
	exit 1
fi
if [ -n "$(git status --porcelain -- .)" ]; then
	echo "commit or stash the changes first" >&2
	exit 1
fi

prefix=github.com/linzeyan/loadconf
export GOWORK=off
dirs=()
for gomod in $(git ls-files '*/go.mod'); do
	dir=$(dirname "$gomod")
	# The tag names the directory, so it must match the module path.
	path=$(cd "$dir" && go list -m) || exit 1
	if [ "$path" != "$prefix/$dir" ]; then
		echo "$dir holds module $path, not $prefix/$dir" >&2
		exit 1
	fi
	dirs+=("$dir")
	# Requires carry a version; replace lines ("path => dir") do not.
	for dep in $(awk -v p="$prefix/" '{for (i = 1; i < NF; i++) if (index($i, p) == 1 && $(i + 1) ~ /^v/) print $i}' "$gomod"); do
		(cd "$dir" && go mod edit -require="$dep@$version") || exit 1
	done
done

# Each go.mod replaces the other modules with their directories, so every
# module builds although none of the new tags exist yet.
for dir in "${dirs[@]}"; do
	(cd "$dir" && go build ./...) || exit 1
done
if [ -n "$(git status --porcelain -- .)" ]; then
	git add -u -- . && git commit -q -m "release $version" || exit 1
fi

tags=()
for dir in "${dirs[@]}"; do
	tag=$dir/$version
	git tag -a "$tag" -m "$prefix/$dir $version" || exit 1
	tags+=("$tag")
done
printf 'tagged %d modules at %s\n' "${#tags[@]}" "$version"
echo "push with: git push origin HEAD ${tags[*]}"
