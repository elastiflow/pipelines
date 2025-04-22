############
# GO vars
############
GO_TEST_COVERAGE_DIR:=$(shell dirname $(realpath $(firstword ${MAKEFILE_LIST})))/coverage
GO_TEST_COVERAGE_DIR_UNIT="${GO_TEST_COVERAGE_DIR}/unit"
GO_TEST_COVERAGE_DIR_INTEGRATION="${GO_TEST_COVERAGE_DIR}/integration"
GO_TEST_COVERAGE_DIR_E2E="${GO_TEST_COVERAGE_DIR}/e2e"
# GO_TEST_COVERAGE_MOD use only package names relative from the repo, so in "REPO_NAME/pkg/subpkg" "REPO_NAME/" should not be passed
GO_TEST_COVERAGE_MOD?=
# GO_TEST_COVERAGE_MOD may be a subdir, replacing / with _ to have a valid filename
GO_TEST_COVERAGE_MOD_COV_FILE:=$(or $(shell echo ${GO_TEST_COVERAGE_MOD} | sed 's/\//_/g'),all)
# TODO(mack#ENG-6493|2025-01-04): Set valid threshold
# GO_TEST_COVERAGE_THRESHOLD"=63.5
GO_TEST_COVERAGE_THRESHOLD:=0
GO_TEST_EXCLUDE_MOD_REGEX?='something_that_does_not_exist_by_default'
GO_TEST_FIND_MOD_CMD:=find . -type f -name 'go.mod' -not -path '*/.terragrunt-cache/*' | grep -vE '${GO_TEST_EXCLUDE_MOD_REGEX}'
# Iterate over modules
define GO_MOD_LOOP
	@${GO_TEST_FIND_MOD_CMD} | while read -r svc; do \
		dir=$$(dirname "$${svc}"); \
		echo 'Running "$(1)" in '$${dir}; \
		(cd $${dir} && $(1)) || exit 1; \
	done
endef

###############
# GO targets
###############
.PHONY: test-unit
test-unit:
	@rm -rf ${GO_TEST_COVERAGE_DIR_UNIT}
	@mkdir -p ${GO_TEST_COVERAGE_DIR_UNIT}
	$(call GO_MOD_LOOP,go test -race -cover -skip="^(TestIntegration|TestE2E).*" ${GO_TEST_OPTS} ./... -args -test.gocoverdir=${GO_TEST_COVERAGE_DIR_UNIT} && echo)

.PHONY: test-unit-verbose
test-unit-verbose: GO_TEST_OPTS="-v"
test-unit-verbose: test-unit

.PHONY: test-unit-pkglist
test-unit-pkglist:
	go tool covdata pkglist -i=${GO_TEST_COVERAGE_DIR_UNIT} -pkg=...${GIT_REPO_NAME}/$(subst .,,${GO_TEST_COVERAGE_MOD})...

.PHONY: test-unit-report-coverage
test-unit-report-coverage:
	go tool covdata func -i=${GO_TEST_COVERAGE_DIR_UNIT} -pkg=...${GIT_REPO_NAME}/$(subst .,,${GO_TEST_COVERAGE_MOD})...

.PHONY: test-unit-report-coverage-html
test-unit-report-coverage-html:
	go tool covdata textfmt -i=${GO_TEST_COVERAGE_DIR_UNIT} \
		-o=${GO_TEST_COVERAGE_DIR}/${GO_TEST_COVERAGE_MOD_COV_FILE}.out \
		-pkg=...${GIT_REPO_NAME}/$(subst .,,${GO_TEST_COVERAGE_MOD})...
	go tool cover -html=${GO_TEST_COVERAGE_DIR}/${GO_TEST_COVERAGE_MOD_COV_FILE}.out \
		-o=${GO_TEST_COVERAGE_DIR}/${GO_TEST_COVERAGE_MOD_COV_FILE}.html
	echo "Coverage report: file://${GO_TEST_COVERAGE_DIR}/${GO_TEST_COVERAGE_MOD_COV_FILE}.html"

.PHONY: test-unit-enforce-coverage
test-unit-enforce-coverage:
	./scripts/test_coverage_threshold.py --makefile $(realpath $(firstword ${MAKEFILE_LIST})) \
		--threshold ${GO_TEST_COVERAGE_THRESHOLD} \
		--go-mods $(shell ${GO_TEST_FIND_MOD_CMD} | xargs dirname | sed 's/^\.\///')

.PHONY: test-integration
test-integration:
	@rm -rf ${GO_TEST_COVERAGE_DIR_INTEGRATION}
	@mkdir -p ${GO_TEST_COVERAGE_DIR_INTEGRATION}
	@$(call GO_MOD_LOOP,go test -race -run=^TestIntegration.* ${GO_TEST_OPTS} ./... && echo)

.PHONY: test-e2e
test-e2e:
	@rm -rf ${GO_TEST_COVERAGE_DIR_E2E}
	@mkdir -p ${GO_TEST_COVERAGE_DIR_E2E}
	@echo TBD

.PHONY: tidy
tidy:
	@echo "Running tidy..."
	@$(call GO_MOD_LOOP,go mod tidy)

.PHONY: staticcheck
staticcheck:
	@echo "Running staticcheck..."
	@$(call GO_MOD_LOOP,staticcheck ./...)
