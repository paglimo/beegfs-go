# Building BeeWatch

This directory contains the files and directory structure needed to build DEB
and RPM packages. Typically packages are built using their Makefile targets:

* `make package-deb`
* `make package-rpm`

When invoked the BeeWatch binary will first be built at
`build/dist/opt/beegfs/bee-watch` then a temporary directory will be created (by
default at `packages/debbuild` or `packages/rpmbuild`) where files from
`build/dist` will be copied in a layout expected by the packaging tools. The
templates in the `DEBIAN` directory or `beewatch.spec.template` will be copied
with `PLACEHOLDER` values filled in at build-time (to reflect the package
version and build time). These final directory structure will then be used to
generate packages using `dpkg-deb` or `rpmbuild`, and the resulting packages
output at `packages/deb` or `packages/rpm`.

If packages are built successfully the build directory will be automatically
cleaned up. Generated packages are not automatically cleaned up until `make
clean` is executed (to allow generating both DEB and RPM packages at the same
time).

# Additional Notes

This approach is modeled after the package targets for BeeGFS itself. Notably we
follow BeeGFS precedence for handling binary/package versioning and generating
changelogs (for additional details on versioning semantics, see the Makefile).
We do deviate slightly with how we build packages by using a temporary
subdirectory so both RPM and DEB packages can be built simultaneously. We're
also able to simplify the process a bit since we're only building a single
binary and there aren't any dependencies (for example a much shorter `.spec`
file).