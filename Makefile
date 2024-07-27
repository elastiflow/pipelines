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
	@go tool cover -func=${GO_TEST_COVERAGE_PROFILE} | grep total


scan\:sbom:
	trivy fs --format cyclonedx .

scan\:sca:
	trivy fs .

scan\:sast:
	semgrep scan --config p/ci && gosec -terse -severity high ./...

scan\:license:
	trivy fs --scanners license --license-full .