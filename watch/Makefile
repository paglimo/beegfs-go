# force the usage of /bin/bash instead of /bin/sh
SHELL := /bin/bash

# List of commands to build:
CMDS = bee-watch

# Variables that can be overridden: 
BUILD_DIR ?= build/dist/opt/beegfs/sbin
PACKAGE_DIR ?= packages/
PACKAGE_DEB_OUT ?= $(PACKAGE_DIR)deb
PACKAGE_RPM_OUT ?= $(PACKAGE_DIR)rpm
PACKAGE_DEB_BUILD ?= $(PACKAGE_DIR)debbuild
PACKAGE_RPM_BUILD ?= $(PACKAGE_DIR)rpmbuild


# Build Targets
all: build generate-notices

.PHONY: build-% build clean generate-notices

# Use the same strategy as BeeGFS to generate a $VERSION for the binary and
# package. The version will look like "1.0.0" if were on a commit tagged
# "1.0.0". Otherwise it will generate a version like "1.0.0-3-g<COMMIT HASH>"
# where "3" is the number of commits ahead of tag 1.0.0 and the <COMMIT HASH> is
# the first ten characters of the commit hash.
$(CMDS:%=build-%): build-%: check-go-version
	$(eval VERSION := $(shell git describe --tags --match '*.*' --abbrev=10))
	@echo "Building $* version $(VERSION)"
	CGO_ENABLED=0 go build -ldflags="-X main.version=$(VERSION) -X main.binaryName=$* -X main.commit=$(shell git rev-parse HEAD) -X main.buildTime=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')" -o $(BUILD_DIR)/$* cmd/$*/main.go
	chmod 755 $(BUILD_DIR)/$*

build: $(CMDS:%=build-%)

generate-notices:
	@go-licenses report ./cmd/bee-watch --template hack/notice.tpl > NOTICE.md --ignore git.beegfs.io --ignore github.com/thinkparq --ignore github.com/hashicorp/hcl

.PHONY: package-deb 
package-deb: build
	@! [ -d '$(PACKAGE_DEB_BUILD)' ] || { echo "Cannot use $(PACKAGE_DEB_BUILD) as the PACKAGE_DEB_BUILD directory (already exists). Please run `make clean` or choose a new directory for PACKAGE_DIR." >&2; false; }
	@echo "Building Debian packages for version $(VERSION)"
	@mkdir -p $(PACKAGE_DEB_BUILD)/DEBIAN
	@cp -r build/dist/* $(PACKAGE_DEB_BUILD)
	@sed 's/VERSION_PLACEHOLDER/$(VERSION)/' build/DEBIAN/control.template > $(PACKAGE_DEB_BUILD)/DEBIAN/control
	@sed 's/VERSION_PLACEHOLDER/$(VERSION)/' build/DEBIAN/changelog.template >  $(PACKAGE_DEB_BUILD)/DEBIAN/changelog
	@sed -i -e 's/DATE_PLACEHOLDER/$(shell date -R)/' $(PACKAGE_DEB_BUILD)/DEBIAN/changelog
	@mkdir -p $(PACKAGE_DEB_OUT)
	@fakeroot chown root:root -R $(PACKAGE_DEB_BUILD)
	@fakeroot dpkg-deb --build $(PACKAGE_DEB_BUILD)/ $(PACKAGE_DEB_OUT)
	@rm -rf $(PACKAGE_DEB_BUILD)

# RPMs don't allow hypens, replace with an underscore the same way we do for BeeGFS.
# We don't use an epoch as this typically only applies if we need to override an older versioning scheme.
.PHONY: package-rpm
package-rpm: build
	@! [ -d '$(PACKAGE_RPM_BUILD)' ] || { echo "Cannot use $(PACKAGE_RPM_BUILD) as the PACKAGE_RPM_BUILD directory (already exists). Please run `make clean` or choose a new directory for PACKAGE_DIR." >&2; false; }
	$(eval BEEGFS_VERSION_RPM := $(subst -,_,$(VERSION)))
	@echo "Building RPM packages for version $(BEEGFS_VERSION_RPM)"
	@mkdir -p $(PACKAGE_RPM_BUILD)/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
	@tar czvf $(PACKAGE_RPM_BUILD)/SOURCES/beewatch.tar.gz --exclude DEBIAN -C build/ dist/
	@sed 's/VERSION_PLACEHOLDER/$(BEEGFS_VERSION_RPM)/' build/beewatch.spec.template > $(PACKAGE_RPM_BUILD)/SPECS/beewatch.spec
	@sed -i -e 's/DATE_PLACEHOLDER/$(shell date "+%a %b %d %Y")/' $(PACKAGE_RPM_BUILD)/SPECS/beewatch.spec
	@rpmbuild --clean -bb $(PACKAGE_RPM_BUILD)/SPECS/beewatch.spec --define '_topdir $(abspath $(PACKAGE_RPM_BUILD))' --define "_rpmdir $(abspath $(PACKAGE_RPM_OUT))"
	@rm -rf $(PACKAGE_RPM_BUILD)

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

# For details on what licenses are disallowed see
# https://github.com/google/go-licenses#check 
#
# IMPORTANT: Any exceptions (using --ignore) such as the one for HCL must be
# manually added AFTER the NOTICE file has been updated and/or other appropriate
# steps have been taken based on the license requirements.
test-licenses: generate-notices
	@echo "Checking license compliance..."
	@go-licenses check ./cmd/bee-watch/ --ignore git.beegfs.io --ignore github.com/thinkparq --ignore github.com/hashicorp/hcl --disallowed_types=forbidden,permissive,reciprocal,restricted,unknown
	@if [ -n "$$(git status --porcelain NOTICE.md)" ]; then \
        echo "NOTICE file is not up to date. Please run 'make generate-notices' and commit the changes."; \
        exit 1; \
    fi

# Targets for installation of various prerequsites:
.PHONY: install-test
install-test: 
	go install github.com/google/go-licenses@latest

# Helper targets:
clean:
	rm -f bin/*
	rm -f build/dist/opt/beegfs/sbin/*
	rm -rf $(PACKAGE_DIR)

tidy :
	@go mod tidy
