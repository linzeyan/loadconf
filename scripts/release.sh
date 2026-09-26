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
./scripts/work.sh || exit 1
mods=()
while read -r path dir; do
	mods+=("$path")
	# Requires carry a version; replace lines ("path => dir") do not.
	for dep in $(awk -v p="$prefix/" '{for (i = 1; i < NF; i++) if (index($i, p) == 1 && $(i + 1) ~ /^v/) print $i}' "$dir/go.mod"); do
		(cd "$dir" && go mod edit -require="$dep@$version") || exit 1
	done
done < <(go list -m -f '{{.Path}} {{.Dir}}')

# The replaces in go.work name the required version, which just changed.
./scripts/work.sh || exit 1
go build "$prefix/..." || exit 1
if [ -n "$(git status --porcelain -- .)" ]; then
	git add -u -- . && git commit -q -m "release $version" || exit 1
fi

tags=()
for mod in "${mods[@]}"; do
	tag=${mod#"$prefix"/}/$version
	git tag -a "$tag" -m "$mod $version" || exit 1
	tags+=("$tag")
done
printf 'tagged %d modules at %s\n' "${#tags[@]}" "$version"
echo "push with: git push origin HEAD ${tags[*]}"
