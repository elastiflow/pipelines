GO_TEST_COVERAGE_PROFILE=coverage.out
GO_TEST_COVERAGE_THRESHOLD=43

.PHONY: test
test:
	GO_TEST_COVERAGE_THRESHOLD=${GO_TEST_COVERAGE_THRESHOLD} \
	go test ${GO_TEST_OPTS} -coverprofile=${GO_TEST_COVERAGE_PROFILE} $$(go list ./... | grep -v examples)

.PHONY: test-verbose
test-verbose: GO_TEST_OPTS="-v"
test-verbose: test

.PHONY: report-coverage
report-coverage:
	make test
	@go tool cover -func=${GO_TEST_COVERAGE_PROFILE} | grep total

.PHONY: coverage-func
coverage-func:
	make test
	go tool cover -func=coverage.out

.PHONY: coverage-html
coverage-html:
	make test
	go tool cover  -html=coverage.out

.PHONY: scan\:sbom
scan\:sbom:
	trivy fs --format cyclonedx .

.PHONY: scan\:sca
scan\:sca:
	trivy fs .

.PHONY: scan\:sast
scan\:sast:
	semgrep scan --config p/ci && gosec -terse -severity high ./...

.PHONY: scan\:license
scan\:license:
	trivy fs --scanners license --license-full .

.PHONY: docs
docs:
	godoc -http=:6060

.PHONY: lint
lint:
	staticcheck ./...