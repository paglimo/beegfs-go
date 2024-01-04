BeeRemote
=========

BeeRemote provides remote data services to BeeGFS file systems. It works by
coordinating distributed operations across one or more worker nodes, such as
BeeSync nodes.


# Development

## Adding new job types and worker nodes

The Mock job and Mock worker node added with [this
commit](https://github.com/ThinkParQ/bee-remote/commit/4828d673f209c7260e69562a52bab6866e967546)
give a good idea what changes are needed to add new job and/or worker node
types.

## Logging

Most job/work requests happen asynchronously and do not immediately return results/errors to the caller. If an error occurs updating a job the error/warning should be logged at the debug level and any relevant details (such as the error) added to the status message of the relevant job and/or job results entry. The only time it is appropriate to log errors or warnings is if the entry cannot be retrieved or updated for some reason.

## Gotchas

### Protocol Buffers and Pointers
While working with pointers inherently carries certain risks, these are further accentuated in our project due to the extensive use of structs defined by protocol buffers. In Go, protocol buffer messages are represented as structs, and messages containing other messages are implemented via fields pointing to structs of the corresponding message types. A common example in our case is the Metadata message, which is used by various message types to convey status information. When creating a new message based on an existing one, an easy mistake is to inadvertently copy references to shared fields (like Metadata) from the original struct. This can lead to unintended side effects, as modifications in the new message could inadvertently affect the original message. To prevent such issues, it's crucial to create independent copies of messages, ensuring that each message is isolated and modifications do not have unintended consequences on other parts of the system.

This is mainly a risk whenever working directly with protobuf defined structs and not going through the Go interfaces and methods like `Status()` which requires modifying the state and message fields directly (which are value types). This does not however prevent inadvertently reusing the pointer returned by `Status()` to create a different protobuf message.

For example when creating a `SyncRequest` for a Job simply doing `wr.Metadata.Status = job.Status()` or `SyncRequest.Metadata: job.baseJob.GetStatus()` (which both return a pointer to the Status message) would copy a reference to the Job's status. This would mean all `SyncRequests` for that job would share the same status, so changes to one would affect all statuses which is not desired. Ensure to create a new instance with values from the old field where appropriate, or use the `proto.Clone()` function (e.g., `Status: proto.Clone(j.Status()).(*flex.RequestStatus),`). 

Note in some cases it may actually be desirable to do this, for example if you are creating `SyncRequests` for the same job if you are 100% certain the referenced message (like `j.Request.GetSync()`) is static and won't change, you may wish to reuse an existing message instead of allocating a new one.
