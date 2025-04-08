
beegfs-go <!-- omit in toc -->
=========

# Contents <!-- omit in toc -->
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Building (With or Without Packaging)](#building-with-or-without-packaging)
  - [Importing functionality into other Go projects](#importing-functionality-into-other-go-projects)
- [FAQs:](#faqs)
  - [How is the project versioned?](#how-is-the-project-versioned)
    - [Why not just use the same version for the Go module and binaries it provides?](#why-not-just-use-the-same-version-for-the-go-module-and-binaries-it-provides)
    - [Is there precedence for this versioning scheme?](#is-there-precedence-for-this-versioning-scheme)
    - [Will eventually the version namespaces collide?](#will-eventually-the-version-namespaces-collide)
    - [Why not just use different tags for the module and binaries?](#why-not-just-use-different-tags-for-the-module-and-binaries)
  - [Can you explain the early development history of this project?](#can-you-explain-the-early-development-history-of-this-project)
    - [Why am I unable to access some issue and PR links?](#why-am-i-unable-to-access-some-issue-and-pr-links)

The purpose of this repository is twofold:

* Provide Go packages for interacting with BeeGFS.
* Provide BeeGFS-related software written in Go, such as the `beegfs` command-line tool.

The overall project is setup as a [Go module](https://go.dev/blog/using-go-modules) which is a
collection of Go packages stored in a file tree with a `go.mod` file at its root:

* `common/`: Contains shared Go packages that can be imported by other projects. These packages
  provide common "low-level" functionality for interacting with BeeGFS and along with basic
  application components (logging, configuration, databases, etc.) and serve as building blocks
  "higher-level" applications and libraries written in Go.
* Other top-level directories (e.g., `ctl/`) contain applications and libraries built on the
  low-level functionality and other components in `common/`. Most developers interested in
  integrating BeeGFS with some external application will want to start here.
  * These directories generally adhere to the unofficial [Standard Go Project
    Layout](https://github.com/golang-standards/project-layout).

IMPORTANT: This repository is not be used for storing protocol buffers as those are not specific to
projects in Go. See the [protocol buffers](https://github.com/thinkparq/protobuf) repository for
`.proto` files and precompiled libraries for Go and other languages.

# Getting Started

## Prerequisites

* If you just want to build/run the project without OS packages, all you need to do is [install
  Go](https://go.dev/doc/install).
* If you want to build packages you also need to [install Go
  Releaser](https://goreleaser.com/install/#go-install).

If you are interested in contributing to the project please refer to [Getting Started with
Go](https://github.com/ThinkParQ/beegfs-go/wiki/Getting-Started-with-Go) in the project wiki.

## Building (With or Without Packaging)

There are a few ways to run the components found in this project:

* Directly build and run (best for debugging): `go run ctl/cmd/beegfs/main.go`

* Install to your `$GOBIN` (best if you just want to run the applications): `go install ./ctl/cmd/beegfs/`
  * For convenience there are also Makefile targets to install/uninstall binaries to `$HOME/go/bin`.
    Use `make install` / `make uninstall` to install everything, or to install a specific binary
    specify `install-<name>` (i.e., for `ctl/` run `make install-ctl` / `make uninstall-ctl`).

* Build and install using OS packages: `make package-all` 
  * Install the resulting packages using `dpkg -i <package>` or similar.

Below replace `ctl/cmd/beegfs/main.go` with the path to the `main.go` file for the component you
want to run. Refer to the documentation included with each component for more details.

## Importing functionality into other Go projects

If you just want to use some common functionality in your project, first run `go get
github.com/thinkparq/beegfs-go@latest` then import/use the shared package(s) as needed throughout
your project. The `beegfs-go` project is meant to be used as a Go module meaning you can (and
should) pin your `go.mod` file to a particular stable version of beegfs-go. See versioning in the
FAQ section below to ensure you use the correct version.

Individual modules can then be imported, for example to use the logging package:

```go
import "github.com/thinkparq/beegfs-go/common/logging"

myConfig := logging.Config{
    Type: "stdout",
    Level: 5,
}

func main() {
  logger, _ := logging.New(myConfig) // Ignore error for brevity.
  logger.Info("This is an informational message")
}
```
For details on using a particular package refer to its documentation. Generally, packages are
documented using Go doc comments, which can be read directly from the source files or with the
command line `go doc` (e.g., `go doc logging`). An interactive doc site can also be started using
the godoc tool (`go get golang.org/x/tools/cmd/godoc`) with `godoc -http=:8080`. Some packages may
also provide additional documentation in markdown format.

# FAQs:

## How is the project versioned?

BeeGFS OS packages/binaries built from this repository and the `beegfs-go` module that provides
reusable Go packages currently follow slightly different versioning schemes:

* OS packages/binaries built from this repository follow the same versioning scheme as the other BeeGFS
  components and start at version `v8.x.y`. 
  * For example if you want to build the version of the `beegfs` tool compatible with BeeGFS 8.0.0
    you would check out the `v8.0.0` tag.
* The Go module and reusable packages it provides will remain at `v0` indicating the Go module's API
  is not necessarily guaranteed to be stable and each `v8.x.y` BeeGFS release, the module's API will
  be versioned as `v0.x.y`.
  * For example, to import Go packages from the `beegfs-go` module that work with BeeGFS `v8.0.0`
    you would run `go get github.com/thinkparq/beegfs-go@v0.0.0`.
  * If you make changes to `beegfs-go` that need to be imported elsewhere before a new "official"
    version is tagged, you would use a [pseudo-version](https://go.dev/ref/mod#pseudo-versions) by
    running `go get github.com/thinkparq/beegfs-go@<LONG-COMMIT-HASH>` to import a specific commit.

### Why not just use the same version for the Go module and binaries it provides?

There are a few reasons for this:

First, Go has specific rules around [module version
numbering](https://go.dev/doc/modules/version-numbers). Once we move past `v0` or `v1`, the module
path must be updated to include the version (i.e., `github.com/thinkparq/beegfs-go/v8`). However, it
would be ideal to provide a single Go module that maintains compatibility across different BeeGFS
versions. This approach simplifies development for maintainers of `beegfs-go` (as there is no need
to backport fixes to multiple branches) and makes it easier for external users. Since we cannot yet
determine if future major BeeGFS versions will require breaking changes to the Go module, there is
no immediate need to synchronize the versions.

Second, moving past `v0` signals that the module's external API is stable and guaranteed not to
change. While we believe the external API is largely stable, we think it is still prudent to wait
before making this guarantee to users.

In short, moving past v0 is a one way trip. Until there is a compelling reason to do so, sticking at
`v0` leaves more options available for how to handle module versioning in the future.

### Is there precedence for this versioning scheme?

Yes, Kubernetes releases are versioned as `v1.31.0`, but the [`kubernetes/client-go`
module](https://github.com/kubernetes/client-go?tab=readme-ov-file#versioning) is versioned as
`v0.31.0`, signaling no API stability is guaranteed (and they do break their API from time to time).

### Will eventually the version namespaces collide? 

If/when we bump from `v0` we would likely either bump to `v1` if we think we can provide one module
that works across multiple major BeeGFS versions, or bump directly to sync with the BeeGFS major
version if backward compatibility cannot be ensured.

### Why not just use different tags for the module and binaries?

We use GoReleaser to build binaries and OS packages. It expects to work with a tag that follows
semantic versioning rules, and currently does not appear to provide a way to extract a semver from a
non-semver tag. However we only run GoReleaser on tags prefixed with `v8.` and the actions workflow
is configured to only run on the pushed tag and determine the previous tag based on the most recent
semantic version before the current tag.

## Can you explain the early development history of this project?

Early development of the components in this repository took place in separate private repositories.
As the project matured and we prepared for the v8.0.0 release, we evaluated the need for this
separation and concluded there was no reason to keep the code split or private. The repositories
were then merged into a unified public repo: `beegfs-go`.

The full commit histories were preserved and stitched together to enable investigation into
architectural decisions made during early development. However, as part of the consolidation
process, the code was reorganized into subdirectories. This means that older commits prior to
`v8.0.0` (or `v0.0.0`) may not build successfully, since their original structure may have changed.

Note those original repositories are archived and read-only. All development now happens on the
public `beegfs-go` repository.

### Why am I unable to access some issue and PR links?

When merging commit histories, we rewrote commit messages to include references to the original
repositories (e.g., `bee-remote/#23`, `bee-watch/#10`) to preserve context. However, these links may
not work for everyone because the original repositories remain private.

We chose not to make those original repositories public due to privacy concerns. For example, to
protect contributor privacy, we updated all commit metadata in this public repo to use GitHub
noreply emails, but sanitizing historical PRs, issues, and branches across multiple private repos is
non-trivial and could still expose private data. If you come across one of these broken links, feel
free to ask in a GitHub Discussion or open an issue if you need clarification.
