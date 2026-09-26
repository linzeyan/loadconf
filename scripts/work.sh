#!/usr/bin/env bash
# Writes go.work, which is generated rather than committed: it joins every
# module of the repository and points the requires between them at the
# working tree. Each replace names the required version because Go rejects an
# unversioned replace of a workspace module, and without one Go still looks
# the version up, which fails for a release commit whose tags do not exist yet.
set -u
cd "$(dirname "$0")/.." || exit 1

prefix=github.com/linzeyan/loadconf
rm -f go.work
{ go work init && go work use -r .; } || exit 1
reqs=$(go work edit -json | awk -F'"' '$2 == "DiskPath" {print $4 "/go.mod"}' |
	xargs awk -v p="$prefix/" '{for (i = 1; i < NF; i++) if (index($i, p) == 1 && $(i + 1) ~ /^v/) print $i "@" $(i + 1)}' |
	sort -u)
for req in $reqs; do
	mod=${req%@*}
	go work edit -replace="$req=./${mod#"$prefix"/}" || exit 1
done
