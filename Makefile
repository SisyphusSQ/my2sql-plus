.PHONY: all build darwin build-all \
	build-linux-amd64 build-linux-arm64 \
	build-darwin-amd64 build-darwin-arm64 build-mac-amd64 build-mac-arm64 \
	build-windows-amd64 build-windows-arm64 \
	test_version deploy clean harness-check harness-verify harness-review-gate

BINARY_NAME = my2sql
APP_VERSION ?= v1.0.0
DIST_DIR ?= bin/release
GO ?= go
MAIN_PKG = my2sql.go

VARS_PKG = github.com/SisyphusSQ/my2sql/internal/vars

BUILD_FLAGS  = -X '${VARS_PKG}.AppName=${BINARY_NAME}'
BUILD_FLAGS += -X '${VARS_PKG}.AppVersion=${APP_VERSION}'
BUILD_FLAGS += -X '${VARS_PKG}.GoVersion=$(shell $(GO) version)'
BUILD_FLAGS += -X '${VARS_PKG}.BuildTime=$(shell date +"%Y-%m-%d %H:%M:%S")'
BUILD_FLAGS += -X '${VARS_PKG}.GitCommit=$(shell git rev-parse HEAD)'
BUILD_FLAGS += -X '${VARS_PKG}.GitRemote=$(shell git config --get remote.origin.url)'

all: clean build-all

build:
	GOARCH=amd64 GOOS=linux $(GO) build -ldflags="${BUILD_FLAGS}" -o bin/${BINARY_NAME} $(MAIN_PKG)

darwin:
	$(GO) build -ldflags="${BUILD_FLAGS}" -o bin/${BINARY_NAME} $(MAIN_PKG)

build-all: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-windows-amd64 build-windows-arm64

build-linux-amd64:
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -ldflags="${BUILD_FLAGS}" -o $(DIST_DIR)/$(BINARY_NAME)-linux-amd64 $(MAIN_PKG)

build-linux-arm64:
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -ldflags="${BUILD_FLAGS}" -o $(DIST_DIR)/$(BINARY_NAME)-linux-arm64 $(MAIN_PKG)

build-darwin-amd64:
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build -ldflags="${BUILD_FLAGS}" -o $(DIST_DIR)/$(BINARY_NAME)-darwin-amd64 $(MAIN_PKG)

build-darwin-arm64:
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build -ldflags="${BUILD_FLAGS}" -o $(DIST_DIR)/$(BINARY_NAME)-darwin-arm64 $(MAIN_PKG)

build-mac-amd64: build-darwin-amd64

build-mac-arm64: build-darwin-arm64

build-windows-amd64:
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build -ldflags="${BUILD_FLAGS}" -o $(DIST_DIR)/$(BINARY_NAME)-windows-amd64.exe $(MAIN_PKG)

build-windows-arm64:
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=windows GOARCH=arm64 $(GO) build -ldflags="${BUILD_FLAGS}" -o $(DIST_DIR)/$(BINARY_NAME)-windows-arm64.exe $(MAIN_PKG)

test_version:
	bin/${BINARY_NAME} version

deploy:
	@install -m 0755 bin/${BINARY_NAME} /usr/local/bin/${BINARY_NAME}

clean:
	@go clean
	@rm -f bin/${BINARY_NAME}
	@rm -rf $(DIST_DIR)

harness-check:
	bash scripts/harness/check.sh

harness-verify: harness-check

harness-review-gate:
	@if [ -z "$(PLAN)" ]; then echo "usage: make harness-review-gate PLAN=path/to/plan.md" >&2; exit 2; fi
	bash scripts/harness/review_gate.sh --plan "$(PLAN)"
