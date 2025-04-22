APP?=""
GIT_REPO_NAME=$(shell basename $$(git rev-parse --show-toplevel))
include mk/*.mk

.PHONY: clean
clean:
	@echo "Running staticcheck..."
	rm -rf ${GO_TEST_COVERAGE_DIR}
	find ./ -type f -name 'coverage.out' -prune -exec rm -rf {} \;

############################################
# All custom targets goes after this line
############################################

.PHONY: scan\:license
scan\:license:
	trivy fs --scanners license --license-full .

.PHONY: docs
docs:
	godoc -http=:6060

.PHONY: lint
lint:
	staticcheck ./...