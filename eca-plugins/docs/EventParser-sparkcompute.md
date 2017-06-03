# Event Parser

EventParser is a SparkCompute plugin, which parses the incoming events and applies any custom transformations.
These custom transformations are defined in the form of Wrangler directives that will be applied on the events.

## Plugin Configuration

Below is a table defining the configurations for this plugin:

    +==================================================================================+
    | Configuration | Required | Default |  Description                                |
    +==================================================================================+
    | Event Field   | Yes      | N/A     | The name of the field containing the event. |
    +==================================================================================+

## Directives

There are 100s of directives and variations supported by the system, please refer to the documentation here : [http://github.com/hydrator/wrangler](http://github.com/hydrator/wrangler)

## Record format

The incoming record should have the following field:
1. A field defined by the plugin configuration, which contains the event string in JSON format.

The output record will have the following fields:
1. A field defined by the plugin configuration, which contains the event string in JSON format.
2. A field named 'key', which defines how to look up the Rule to be applied to the event.
