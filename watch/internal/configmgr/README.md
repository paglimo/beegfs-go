ConfigManager
=============

# Adding New Configuration

NOTE: When adding a new configuration parameter to the struct where the components configuration is defined (usually `Config`) the field must be exported and requires a `mapstructure` field tag that defines the external user facing setting. Note the standard to prefix field tags with the subscriber type to avoid conflicts, though right now we don't require this for the internal fields.

## Add a configuration option to a particular component
*Example based on the logger, most components would follow this pattern.*

In `logger/logger.go` in `Config` add your new field. If you want to be able to adjust the log level using a flag add it in main.go and also specify the default. If you want the log manager to validate the provided values when the configuration is initially set or updated, add any checks to `validateConfig()` in `configmgr/config.go`.

## Add configuration to an existing subscriber type 

*Example based on the gRPC subscriber type, all subscribers should follow this pattern so simply amend as needed.*

In `subscriber/config.go` in`GRPCSubscriber` add your new field. 
In `grpc.go` amend `newGRPCSubscriber` as needed to validate input and/or set reasonable defaults as needed. The variable can then be used in the GRPCSubscriber methods as needed. Note by default all fields will be unmarshalled by ConfigMgr into their default Golang type values (i.e., for an integer this would be 0 or a bool false). 