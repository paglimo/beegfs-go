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