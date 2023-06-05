# force the usage of /bin/bash instead of /bin/sh
SHELL := /bin/bash

# List of commands to build:
CMDS = bee-watch

# Build Targets
all: protos build

check-go-version:
	./hack/check-go-version.sh

.PHONY: build-% build clean

$(CMDS:%=build-%): build-%: check-go-version
	go build -o bin/$* cmd/$*/main.go

build: $(CMDS:%=build-%)

# Generate Go code from protobuf files:
PROTO_FILES := $(shell find ./. -name '*.proto')
PROTO_GENERATED_FILES := $(patsubst %.proto, %.pb.go, $(PROTO_FILES))

# TODO: Update this to account for proto files being under both api/ and internal/api.

.PHONY: protos

protos: $(PROTO_GENERATED_FILES)

%.pb.go: %.proto
	@echo "Compiling $<"
	protoc -I $(dir $<) --go_out=$(dir $<) --go_opt=paths=source_relative --go-grpc_out=$(dir $<) --go-grpc_opt=paths=source_relative $<

# Test targets: 
# Test targets may make change to the local repository (e.g. running go mod tidy) to
# verify all code required to build the project has been properly committed.
# Commonly this is done by running `make test` in CI, but could also be done locally.
# If you ran `make test` locally you may want to use `git reset` to revert the changes.
.PHONY: test test-gofmt test-go-tidy test-go-vendor test-protos test-licenses
test: test-gofmt test-gofmt test-go-tidy test-go-vendor test-protos test-licenses

# Verify that the code is formatted using gofmt: 
# Don't run on the vendor directory to avoid false positives.
test-gofmt:
	@if [ -n "$$(gofmt -l ./ | grep -v vendor)" ]; then \
		echo "The following files have not been formatted using gofmt:"; \
		echo -e "\nFix individual files with: \ngofmt -w <file> \n"\
		"\nOr fix all files with:\nfind . -type d \( -path './vendor' \) -prune -o -name '*.go' -print0 | xargs -0 gofmt -w \n\n" \
		exit 1; \
	fi

# Verify that go mod tidy has been run.
test-go-tidy: tidy
	@if [ -n "$$(git status --porcelain go.mod go.sum)" ]; then \
		echo "go.mod and/or go.sum are not up to date. Please run 'go mod tidy' and commit the changes."; \
		exit 1; \
	fi

test-go-vendor:
	@if [ -n "$$(git status --porcelain vendor)" ]; then \
        echo "Vendor directory is not up to date. Please run 'go mod vendor' and commit the changes."; \
        exit 1; \
    fi

test-protos: protos
	@if [ -n "$$(git status --porcelain */pb/*)" ]; then \
		echo "Protobuf files are not up to date. Please run 'make protos' and commit the changes."; \
		exit 1; \
	fi

# For details on what licenses are disallowed see https://github.com/google/go-licenses#check
test-licenses: 
	go-licenses check ./cmd/bee-remote/ --ignore git.beegfs.io/beeflex/bee-remote/cmd/bee-remote --ignore git.beegfs.io/beeflex/bee-remote/internal/api/v1 --disallowed_types=forbidden,permissive,reciprocal,restricted,unknown

# Targets for installation of various prerequsites:
.PHONY: install-test
install-test: 
	go install github.com/google/go-licenses@latest

# Helper targets:
clean :
	rm -f bin/*
	rm -f api/proto/v1/*.go

tidy :
	go mod tidy
