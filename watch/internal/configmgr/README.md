ConfigManager
=============

# Adding New Configuration

## Add configuration to an existing subscriber type 

*Example based on the gRPC subscriber type, all subscribers should follow this pattern so simply amend as needed.*

In `subscriber/config.go` in`GRPCSubscriber` add your new field. Note the requirement for the field to be exported and to define a `mapstructure` field tag that defines the external user facing setting. Note the standard to prefix field tags with the subscriber type to avoid conflicts, though right now we don't require this for the internal fields.

In `grpc.go` amend `newGRPCSubscriber` as needed to validate input and/or set reasonable defaults as needed. The variable can then be used in the GRPCSubscriber methods as needed. Note by default all fields will be unmarshalled by ConfigMgr into their default Golang type values (i.e., for an integer this would be 0 or a bool false). 