Remote Storage Targets
======================

Remote Storage Targets provides remote data services for BeeGFS. It works by using a single BeeGFS
Remote service to coordinates distributed data synchronization using one or more BeeGFS Sync worker
nodes. These services can be deployed on the same machine(s) as other BeeGFS services, or on
dedicated machines. The Remote and Sync services can also be deployed to the same machine.