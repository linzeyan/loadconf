# Tests need nothing but Go. Each module is vetted and tested on its own,
# several at a time (scripts/each-module.sh): every go.mod replaces the other
# modules of this repository with their directories (scripts/replaces.sh), so
# no workspace is needed. GOWORK=off
# keeps a local go.work, such as one made for gopls, from changing what is
# tested. make release VERSION=vX.Y.Z tags every module at that version (see
# CONTRIBUTING.md).
export GOWORK := off
MODULES := $(patsubst %/go.mod,%,$(shell git ls-files --cached --others --exclude-standard '*/go.mod'))

.PHONY: lint test release

# A go.sum that lists a module of this repository means a require of it has
# no replace, so that module was built from the proxy, not the working tree.
lint: ## Vet every module and check that requires between them are replaced
	@if grep -l 'github.com/linzeyan/loadconf/' $(addsuffix /go.sum,$(MODULES)) 2>/dev/null; then \
		echo "these go.sum files list modules of this repository: run scripts/replaces.sh and go mod tidy" >&2; exit 1; fi
	@./scripts/each-module.sh go vet ./...

test: ## Run the tests of every module with the race detector
	@./scripts/each-module.sh go test -race ./...

release:
	./scripts/release.sh $(VERSION)
