# Building BeeWatch

## OS Packages 

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

### Additional Notes

This approach is modeled after the package targets for BeeGFS itself. Notably we
follow BeeGFS precedence for handling binary/package versioning and generating
changelogs (for additional details on versioning semantics, see the Makefile).
We do deviate slightly with how we build packages by using a temporary
subdirectory so both RPM and DEB packages can be built simultaneously. We're
also able to simplify the process a bit since we're only building a single
binary and there aren't any dependencies (for example a much shorter `.spec`
file).

## (Docker) Container Images

The Dockerfile at the repository root can be used to build a container image.
Typically the image is built using the Makefile target and the image tag is set
automatically based on the generated version (see the Makefile for details on
how the version is generated):

* `make package-docker`

Here is a simple example of how to use the resulting image given a metadata
service logging events on the base OS at
`sysFileEventLogTarget=/run/beegfs/eventlog` and a subscriber listening on the
default Docker bridge network at `172.17.0.1:50052`. We'll start with the full
command then we'll break it down step-by-step:

```shell
docker run \
    -v /run/beegfs:/run/beegfs \
    bee-watch:0.0.1-3-g5bd27fa046 --metadata.eventLogTarget=/run/beegfs/eventlog --subscribers="id=1,name='subscriber1',type='grpc',grpcHostname='172.17.0.1',grpcPort=50052,grpcAllowInsecure=true"
```

Breaking it down: 

(1) The `-v /run/beegfs:/run/beegfs` bind mounts the /run/beegfs directory on
the host machine's filesystem into the container. This is what allows the
containerized BeeWatch service to access the Unix socket where the metadata
service expects to output events.

(2) The `bee-watch:0.0.1-3-g5bd27fa046` is the name of the container image built
using `make package-docker` (see `docker images` if you're not sure the name).

(3) The rest of the command are regular arguments passed to BeeWatch. First the
path to `sysFileEventLogTarget` as
`--metadata.eventLogTarget=/run/beegfs/eventlog`. Then the subscriber, notably
providing the IP and host of the subscriber that is listening on the Docker
bridge network:
`--subscribers="id=1,name='subscriber1',type='grpc',grpcHostname='172.17.0.1',grpcPort=50052,grpcAllowInsecure=true"`

Here we provided all necessary BeeWatch arguments as flags, but they can also be
specified using environment variables or a configuration file. For example we
could have specified the `--metadata.eventLogTarget` as an environment variable: 

 ```shell
docker run \
    -v /run/beegfs:/run/beegfs \
    -e BEEWATCH_METADATA_EVENTLOGTARGET=/run/beegfs/eventlog \
    bee-watch:0.0.1-3-g5bd27fa046 --subscribers="id=1,name='subscriber1',type='grpc',grpcHostname='172.17.0.1',grpcPort=50052,grpcAllowInsecure=true"
```

We could also have bind mounted a configuration file into the container, if we
wanted to dynamically update the subscriber configuration:

 ```shell
docker run \
    -v ./build/dist/etc/beegfs:/etc/beegfs \
    -v /run/beegfs:/run/beegfs \
    -e BEEWATCH_METADATA_EVENTLOGTARGET=/run/beegfs/eventlog \
    bee-watch:0.0.1-3-g5bd27fa046 --cfgFile=/etc/beegfs/bee-watch.toml --log.type=stdout
```
> The default configuration file sets the log.type to "logfile", but
typically containers are setup to log to stdout which is why we override it here
using a flag. Adjust as needed based on your requirements.

The ability to use a mix of flags, environment variables, and a configuration
file provides flexibility when running BeeWatch in a container.