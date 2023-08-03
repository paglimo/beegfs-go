th# BeeWatch

BeeWatch is a replacement for the [beegfs-event-listener](https://doc.beegfs.io/latest/advanced_topics/filesystem_modification_events.html#beegfs-event-listener).

# Usage

## Shutdown 

The application is wired to shutdown on a SIGTERM or SIGINT. The first signal attempts a clean shutdown by first disconnecting the metadata service but continuing to accept configuration updates and sending outstanding events in the buffer to all subscribers. If a second signal is received, the application will attempt to immediately shutdown by disconnecting all subscribers and exiting, thus dropping any outstanding unacknowledged events in the buffer. If the metadata service is not also buffering events these events will be lost forever.

Currently the clean shutdown is setup so events must be sent to all subscribers regardless if they were connected or not when the shutdown was initiated. This means the first signal could hang forever if disconnected subscriber(s) never reconnect to receive their outstanding events. If you want to be sure events were sent to certain subscriber(s) you can update the subscriber configuration while the application is shutting down, for example to correct misconfiguration or remove a subscriber.