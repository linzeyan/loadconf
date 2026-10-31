#!/usr/bin/env bash
# Runs a command in every module of this repository, JOBS modules at a time
# (default: half the CPUs; each go command is parallel itself), and prints
# each module's output in one piece, in path order. One go test over all
# modules needed a workspace; without it, running the modules one by one
# took three times as long. Exits non-zero when the command failed anywhere.
#
# Usage: scripts/each-module.sh go test -race ./...
set -u
cd "$(dirname "$0")/.." || exit 1
if [ $# -eq 0 ]; then
	echo "usage: $0 command [args...]" >&2
	exit 2
fi

out=$(mktemp -d) || exit 1
trap 'rm -rf "$out"' EXIT
export out
run() {
	local dir=$1 log
	shift
	log="$out/${dir//\//_}"
	(cd "$dir" && "$@") >"$log" 2>&1 || touch "$log.failed"
}
export -f run

mods=$(git ls-files --cached --others --exclude-standard '*/go.mod' | xargs -n1 dirname | sort)
jobs=${JOBS:-$((($(getconf _NPROCESSORS_ONLN) + 1) / 2))}
printf '%s\n' $mods | xargs -P "$jobs" -I{} bash -c 'run "$@"' _ {} "$@"

failed=()
for dir in $mods; do
	cat "$out/${dir//\//_}"
	if [ -e "$out/${dir//\//_}.failed" ]; then
		failed+=("$dir")
	fi
done
if [ ${#failed[@]} -gt 0 ]; then
	echo "failed in: ${failed[*]}" >&2
	exit 1
fi
