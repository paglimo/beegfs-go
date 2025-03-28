# Building BeeWatch

Packages and container images for BeeGFS Watch are built using GoReleaser:

* To build packages simply run `make package-all`.
* To build Docker image you could build those directly using `docker build` or using GoReleaser with
  a command like: `goreleaser --clean --snapshot --skip sign,nfpm`
