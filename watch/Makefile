# force the usage of /bin/bash instead of /bin/sh
SHELL := /bin/bash

# List of commands to build:
CMDS = bee-watch

# Build Targets
all: build

.PHONY: build-% build clean

$(CMDS:%=build-%): build-%: check-go-version
	go build -o bin/$* cmd/$*/main.go

build: $(CMDS:%=build-%)

# Test targets: 
# Test targets may make change to the local repository (e.g. running go mod tidy) to
# verify all code required to build the project has been properly committed.
# Commonly this is done by running `make test` in CI, but could also be done locally.
# If you ran `make test` locally you may want to use `git reset` to revert the changes.
.PHONY: test check-go-version test-gofmt test-go-tidy test-go-vendor test-licenses
test: check-go-version test-gofmt test-go-tidy test-go-vendor test-licenses

check-go-version:
	@echo "Checking Go version..."
	@./hack/check-go-version.sh

# Verify that the code is formatted using gofmt: 
# Don't run on the vendor directory to avoid false positives.
test-gofmt:
	@echo "Checking 'go fmt' has been run..."
	@if [ -n "$$(gofmt -l ./ | grep -v vendor)" ]; then \
		echo "The following files have not been formatted using gofmt:"; \
		echo -e "\nFix individual files with: \ngofmt -w <file> \n"\
		"\nOr fix all files with:\nfind . -type d \( -path './vendor' \) -prune -o -name '*.go' -print0 | xargs -0 gofmt -w \n\n" \
		exit 1; \
	fi

# Verify that go mod tidy has been run.
test-go-tidy: tidy
	@echo " Checking 'go mod tidy' has been run..."
	@if [ -n "$$(git status --porcelain go.mod go.sum)" ]; then \
		echo "go.mod and/or go.sum are not up to date. Please run 'go mod tidy' and commit the changes."; \
		exit 1; \
	fi

test-go-vendor:
	@echo "Checking 'go mod vendor' has been run..."
	@if [ -n "$$(git status --porcelain vendor)" ]; then \
        echo "Vendor directory is not up to date. Please run 'go mod vendor' and commit the changes."; \
        exit 1; \
    fi

# For details on what licenses are disallowed see https://github.com/google/go-licenses#check
test-licenses: 
	@echo "Checking license compliance..."
	@go-licenses check ./cmd/bee-watch/ --ignore git.beegfs.io --ignore github.com/thinkparq --disallowed_types=forbidden,permissive,reciprocal,restricted,unknown

# Targets for installation of various prerequsites:
.PHONY: install-test
install-test: 
	go install github.com/google/go-licenses@latest

# Helper targets:
clean :
	rm -f bin/*
	rm -f api/proto/v1/*.go

tidy :
	@go mod tidy
