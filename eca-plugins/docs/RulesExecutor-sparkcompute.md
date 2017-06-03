# Rules Executor

RulesExecutor is a SparkCompute plugin, which applies rules against the incoming events. These rules are defined
in the form of JEXL expressions which are expected to return boolean values.

## Plugin Configuration

Below is a table defining the configurations for this plugin:

    +==================================================================================+
    | Configuration | Required | Default |  Description                                |
    +==================================================================================+
    | Event Field   | Yes      | N/A     | The name of the field containing the event. |
    +==================================================================================+

## Record format

The incoming record should have the following fields:
1. A field named 'key', of type String, which defines how to look up the Rule to be applied to the event.
2. A field defined by the plugin configuration, which contains the event string in JSON format.

The output record will have the following fields:
1. A field defined by the plugin configuration, which contains the event string in JSON format.
2. For each action defined by the rule for the event, the field will be the action and the value will be the
result of the condition, after having been applied to the event.
