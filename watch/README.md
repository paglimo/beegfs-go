BeeWatch <!-- omit in toc -->
========

# Contents <!-- omit in toc -->

- [About](#about)
- [Quick Start (for developers)](#quick-start-for-developers)
- [Configuring BeeWatch](#configuring-beewatch)
  - [Overview](#overview)
    - [Specify Configuration Using Flags](#specify-configuration-using-flags)
    - [Specify Configuration Using Environment Variables](#specify-configuration-using-environment-variables)
    - [Specify Configuration using a TOML Configuration File](#specify-configuration-using-a-toml-configuration-file)
  - [Configuring Subscribers](#configuring-subscribers)
    - [gRPC](#grpc)
  - [Updating Configuration (without restarting BeeWatch)](#updating-configuration-without-restarting-beewatch)
- [Shutting Down BeeWatch](#shutting-down-beewatch)
- [Advanced](#advanced)
  - [Developing gRPC Subscribers](#developing-grpc-subscribers)

# About

BeeWatch is a replacement for the [beegfs-event-listener](https://doc.beegfs.io/latest/advanced_topics/filesystem_modification_events.html#beegfs-event-listener). It handles receiving file system modification events from a single BeeGFS metadata service over a Unix socket then forwarding them to one or more subscribers. Currently BeeWatch only supports gRPC subscribers, but the design is flexible so additional subscriber types can be easily added in the future such as InfluxDB. 

BeeWatch is responsible for avoiding lost events due to network or other remote issues, for example if the server a subscriber is on reboots for updates. It does so by buffering each event received from the BeeGFS metadata service in-memory until the event is sent to all subscribers, or its internal event buffer overflows (then the oldest event(s) will be dropped). It is not responsible for avoiding lost events due to the server it runs on rebooting/crashing for any reason, this will instead be handled by future changes to the BeeGFS metadata service to add an on-disk event buffer that will be used to recreate the BeeWatch in-memory buffer after a planned/unplanned restart. When supported by subscribers it can also guarantee a particular event is only ever sent to a subscriber once.

# Quick Start (for developers)
*This guide presumes you are not working with a prebuilt BeeWatch binary, and instead have cloned the repository locally and already have [Go installed](https://go.dev/doc/install).*

For BeeWatch to start the only required option is the path to the Unix socket where the BeeGFS metadata server will log file system modification events (specified in `/etc/beegfs/beegfs-meta.conf` as `sysFileEventLogTarget`). This file doesn't have to exist before BeeWatch is started, and generally BeeWatch should be started before the metadata service is actually sending events to the socket. To start BeeWatch from the `cmd/bee-watch/` directory run:

`go run main.go --metadata.eventLogTarget=<PATH>`

At this point BeeWatch will begin buffering any events it receives from the metadata service until it reaches the default `--metadata.eventBufferSize`, then the oldest events will start to be dropped. This intentional default behavior keeps as many historical events as possible so subscribers can be added after BeeWatch has started. To allow subscribers to be configured without restarting BeeWatch we need to start it with a configuration file:

1. Stop BeeWatch if it is still running (Ctrl+C). 
2. Create an empty TOML file, for example: `touch beewatch.toml`
3. Restart BeeWatch: `go run main.go --metadata.eventLogTarget=<PATH> --cfgFile=beewatch.toml`
4. Add a subscriber to the `beewatch.toml` file. Note this subscriber doesn't actually have to exist if you just want to experiment with BeeWatch (it will simply log unable to connect):
  ```toml
  [[subscriber]]
  id = 1
  name = 'test-subscriber'
  type = 'grpc'
  grpcHostname = 'localhost'
  grpcPort = '50052'
  grpcAllowInsecure = true
  ```
5. From a new terminal send BeeWatch hang up signal:
   1. Determine the BeeWatch process ID by running `pgrep -a main` and ensuring the only PID returned is for BeeWatch (the easiest way to tell is based on the flags as the executable will look like `/tmp/go-build2521339517/b001/exe/main`).
   2. Run `kill -HUP $(pgrep main)` or run `kill -HUP <PID>` with the appropriate process ID.
6. In the original terminal you should observe BeeWatch log it is adding a subscriber and start attempting to connect. If the subscriber doesn't exist BeeWatch will continue trying to reconnect with an exponential back off.

# Configuring BeeWatch

## Overview

BeeWatch supports multiple configuration sources, and more than one source can be used at once. Except for the subscriber configuration, options can be specified in multiple places, and the final configuration is determined based on the following precedence order: 

1. Command line flags
2. Environment variables
3. TOML Configuration file
4. Built-in defaults

This provides flexibility in defining a base application configuration that can be changed at runtime based on the environment (for example to set different log destinations in production versus pre-production). For a list of all available configuration options including their defaults see `--help`. 

### Specify Configuration Using Flags

BeeWatch can be fully configured to run using command line flags. See `--help` for all options.

### Specify Configuration Using Environment Variables 

All options listed in `--help` can be specified as environment variables, but option names needs to be reformated to be valid/standard Linux variable names by applying the following rules:

* All variables that apply to BeeWatch must be prefixed with `BEEWATCH_`.
* All letters must be uppercase. 
* All dots (.) in the parameter name must be replaced with underscores (_).

For example the flag `--log.type` would be specified as `BEEWATCH_LOG_TYPE`.

### Specify Configuration using a TOML Configuration File

When this option is used BeeWatch should be started with the flag `--cfgFile=<PATH>` where `<PATH>` is an absolute path to the configuration file to use. Except for `--cfgFile` all options specified in `--help` can be specified in a configuration file. Options are specified based on TOML formatting rules:

* The list of subscribers (`--subscribers`) is specified as individual subscribers using a TOML array (`[[subscriber]]`) with options for that subscriber specified beneath as `key=value`. For example: 

```toml
[[subscriber]]
id = 1
name = 'subscriber-1'
type = 'grpc'
grpcHostname = 'localhost'
[[subscriber]]
id = 2
name = 'subscriber-2'
type = 'grpc'
grpcHostname = 'localhost'
```

* For all other options, the   part of each option name (preceding the dot) indicates what internal component the configuration is directed at, and is specified once as a table header followed by the configuration options for that component as `key=value`. For example to specify the `--log.type`, `log.file`, and `log.level` in TOML:
```toml
[log]
type = 'logfile'
file = '/var/log/beewatch/beewatch.log'
level = 3
```

All values must be specified using the rules for TOML [key/value pairs](https://toml.io/en/v1.0.0#keyvalue-pair), notably:

* All keys are specified as is and followed by an equals sign and their value.
* Values are specified based on the type they represent:
  * All strings are surrounded by single 'quotes'. 
  * All numbers are listed with no quotes.
  * All boolean (true/false) values are listed with no quotes. 

## Configuring Subscribers

When configuring subscribers, the following options must be specified for each subscriber regardless of type: 

* id
* name
* type

The `id` option is used to keep track of what events should/have been sent to a particular subscriber. This means all other subscriber configuration (including the type) can be changed without losing events intended for a particular subscriber as long as the ID remains the same. For example if an event with sequence ID 5 was sent and acknowledged by subscriber "1", then subscriber "1" was disconnected and reconfigured with a new hostname (perhaps due to a server migration), when we reconnect we'll continue sending events starting with sequence ID 6 (presuming no events were dropped due to a buffer overflow). The `name` option no special significance and is intended to help users identify this subscriber in logs and configuration files. The `type` option is used to specify what protocol should be used to communicate with this subscriber. Additional options will be needed based on the provided type. 

### gRPC

To use the gRPC subscriber type specify `type: grpc` then specify the following options:

* grpcHostname (required): The IP address or hostname of the subscriber. No default.
* grpcPort (required): What port the subscriber is listening on. No default.
* grpcAllowInsecure (optional): Disables use of TLS meaning all gGRPC message are sent in clear text. Default: false.
* grpcDisconnectTimeout (optional): When a local disconnect is requested, how long to wait (in seconds) for the remote subscriber to also shutdown their end of the connection before just closing the connection. If this value is to short then we may not receive acknowledgement of events the subscriber has processed (subscribers should resend the acknowledgement when they reconnect). If this value is to long, then reconfiguration attempts or shutting down BeeWatch may hang for an inconvenient amount of time. Default: 30 (seconds). 

## Updating Configuration (without restarting BeeWatch)

Currently the following configuration can be updated after BeeWatch has started without requiring a restart:

* Subscribers
* Log level

The primary intent is to allow subscribers to be added, removed, and updated without impacting other subscribers or dropping events. Allowing the log level to be updated on-the-fly simplifies troubleshooting issues without requiring a full restart, which may not be possible, or may loose state that makes issues difficult to reproduce. 

Only configuration set using a configuration file can be updated without a restart. Because flags and environment variables cannot be updated once the application has started (and they have the highest precedence), their configuration is immutable. To update the configuration first make the appropriate changes to the configuration file then send the BeeWatch process a signal hang up (SIGUP). For example by running `kill -HUP <PID>` where PID is the process ID from `pgrep`. 

# Shutting Down BeeWatch 

The application is wired to shutdown on a SIGTERM or SIGINT. The first signal attempts a clean shutdown by first disconnecting the metadata service but continuing to accept configuration updates and sending outstanding events in the buffer to all subscribers. If a second signal is received, the application will attempt to immediately shutdown by disconnecting all subscribers and exiting, thus dropping any outstanding unacknowledged events in the buffer. If the metadata service is not also buffering events these events will be lost forever.

Currently the clean shutdown is setup so events must be sent to all subscribers regardless if they were connected or not when the shutdown was initiated. This means the first signal could hang forever if disconnected subscriber(s) never reconnect to receive their outstanding events. If you want to be sure events were sent to certain subscriber(s) you can update the subscriber configuration while the application is shutting down, for example to correct misconfiguration or remove a subscriber.

# Advanced

## Developing gRPC Subscribers

<!-- TODO -->
