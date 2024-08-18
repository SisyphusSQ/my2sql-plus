BINARY_NAME = my2sql

VARS_PKG = github.com/SisyphusSQ/my2sql/internal/vars

BUILD_FLAGS  = -X '${VARS_PKG}.AppName=${BINARY_NAME}'
BUILD_FLAGS += -X '${VARS_PKG}.AppVersion=0.0.1'
BUILD_FLAGS += -X '${VARS_PKG}.GoVersion=$(shell go version)'
BUILD_FLAGS += -X '${VARS_PKG}.BuildTime=$(shell date +"%Y-%m-%d %H:%M:%S")'
BUILD_FLAGS += -X '${VARS_PKG}.GitCommit=$(shell git rev-parse HEAD)'
BUILD_FLAGS += -X '${VARS_PKG}.GitRemote=$(shell git config --get remote.origin.url)'

all: clean build deploy

build:
	GOARCH=amd64 GOOS=linux go build -ldflags="${BUILD_FLAGS}" -o bin/${BINARY_NAME} my2sql.go

test:
	go build -ldflags="${BUILD_FLAGS}" -o bin/${BINARY_NAME} my2sql.go

test_version:
	bin/${BINARY_NAME} version

deploy:
	@mv -f ${BINARY_NAME} /usr/local/bin/

clean:
	@go clean
	@rm -f bin/${BINARY_NAME}