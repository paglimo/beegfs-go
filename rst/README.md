BeeRemote
=========

BeeRemote provides remote data services to BeeGFS file systems. It works by
coordinating distributed operations across one or more worker nodes, such as
BeeSync nodes.


# Development

The Mock job and Mock worker node added with [this
commit](https://github.com/ThinkParQ/bee-remote/commit/4828d673f209c7260e69562a52bab6866e967546)
give a good idea what changes are needed to add new job and/or worker node
types.