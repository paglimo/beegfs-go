Config
======

There are two objectives driving how we manage configuration:

* Minimize the number of changes required when adding/removing/changing
  configuration parameters. For example when we add a parameter the only changes
  required are extending the config struct on the component that requires the
  new parameter, adding the parameter to the New function for the type
  implementing the parameter, then optionally adding static verification checks
  to the config package.
* Each component package of the application is able to define the configuration
  it works on to provide encapsulation. This way individual packages don't rely
  on any state/configuration outside what is defined in the immediate package,
  making it easy to unit test and swap out individual package implementations
  without side effects. 

To achieve this, the Config package combines all application configuration into a
single AppConfig struct using composition. It does this to simplify unmarshaling
configuration sources into the appropriate config using ConfigManager (and Viper
under the hood). The AppConfig struct satisfies the ConfigManager's Configurable
interface meaning it provides functionality to:

* Return a new empty instance of AppConfig.
* Determine if an old AppConfig can be updated to a new AppConfig.
* Determine if the provided AppConfig has allowed values set.

It also implements the Configurer interface which is required by any components
that implement ConfigManager's Listener interface to support dynamic
configuration updates. This means it provides methods that return only a subset
of the configuration intended to be used by a particular component (typically
the one that defined the configuration).

# Adding New Configuration

NOTE: When adding a new configuration parameter to the struct where the
components configuration is defined (usually `Config`) the field must be
exported and requires a `mapstructure` field tag that defines the external user
facing setting. When defining different subscriber types the standard is to
prefix field tags with the subscriber type to avoid conflicts, though right now
we don't require this for the internal fields.

## Add a configuration option to a particular component
*Example based on the logger, most components would follow this pattern.*

In `logger/logger.go` in `Config` add your new field. If you want to be able to
adjust the log level using a flag add it in main.go and also specify the
default. If you want the log manager to validate the provided values when the
configuration is initially set or updated, add any checks to `validateConfig()`
in `config/config.go`.

## Add configuration to an existing subscriber type 

*Example based on the gRPC subscriber type, all subscribers should follow this
pattern so simply amend as needed.*

In `subscriber/config.go` in`GRPCSubscriber` add your new field. In `grpc.go`
amend `newGRPCSubscriber` as needed to validate input and/or set reasonable
defaults as needed. The variable can then be used in the GRPCSubscriber methods
as needed. Note by default all fields will be unmarshalled by ConfigMgr into
their default Golang type values (i.e., for an integer this would be 0 or a bool
false). 