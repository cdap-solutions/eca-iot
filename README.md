# ECA (Event-Condition-Action)

ECA is an application that leverages CDAP in order to implement a pipeline where events are parsed,
conditions are applied on those events, and actions are executed if those conditions are met.

This application consists of three primary CDAP components:

1. service: A CDAP Service, which is used for defining Schemas and Rules.

2. EventParser: A SparkCompute plugin, which parses the incoming events and applies any custom transformations.
These custom transformations are defined in the form of Wrangler directives that will be applied on the events.

3. RulesExecutor: A SparkCompute plugin, which applies rules against the incoming events. These rules are defined
in the form of JEXL expressions which are expected to return boolean values.


# Guide

This guide will demonstrate how to setup an ECA application, defining schemas and rules, and
process events using this application via step-by-step instructions.

# Summary of steps

1. Package the ECAApp and the SparkCompute plugins
2. Deploy the ECAApp application jar as a CDAP application
3. Deploy the ECA Plugins
4. Define Schemas and Rules (conditions)
5. Create a CDAP Application using the deployed plugins
6. Run the application in CDAP
7. Ingest the events
8. View the output



## Step 1 - Check out and build the project:

```
git clone https://github.com/caskdata/eca-iot.git
cd eca-iot
mvn clean package
```


## Step 2 - Deploy ECAApp and start the CDAP Service:

```
cdap cli deploy app eca-app/target/eca-app-1.2.0-SNAPSHOT.jar
cdap cli start service eca.service
```


## Step 3 - Deploy ECA Plugins

CDAP plugin architecture allows reusing existing code to create new CDAP application. Deploy ECA Plugins in CDAP as an artifact:

```
cdap cli load artifact eca-plugins/target/eca-plugins-1.2.0-SNAPSHOT.jar config-file eca-plugins/target/eca-plugins-1.2.0-SNAPSHOT.json
```


## Step 4 - Define a Schema and add a Rule

Schemas define the transformations that must be applied to an event in order for it to be interpreted by the Rules Executor plugin.
These transformations are in the form of Wrangler directives.
Note that the 'key' field needs to be defined in the event, because is used to lookup rules in the Rules Executor plugin.

Add a schema that is defined by the four fields [name, device_id, ts, heart_rate]:

```
cdap cli call service eca.service PUT /schemas/foo body '{"uniqueFieldNames": ["name", "device_id", "ts", "heart_rate"], "directives": ["rename device_id key"]}'
```

Rules define a set of conditions and corresponding actions to be taken in the case that the conditions evaluate to true.

Add rules that are defined for events with key=='device001' and key='device002':

```
cdap cli call service eca.service PUT /rules/device001 body '{"conditions": [{condition: "heart_rate > 100", actionType: "sms"}]}'
cdap cli call service eca.service PUT /rules/device002 body '{"conditions": [{condition: "heart_rate > 105", actionType: "sms"}]}'

```


## Step 5 - Deploy and start a Spark Realtime Streaming pipeline

Use the JSON configuration file located at demos/app.json for configuring and deploying a CDAP application:

```
cdap cli create app rulesExecutor cdap-data-streams 4.1.0 SYSTEM demos/app.json
cdap cli start spark rulesExecutor.DataStreamsSparkStreaming
```


## Step 6 - Ingest data in the source

In this example, the pipeline is reading events from Kafka, but the events could be configured to be read from any
other Realtime source also.
Populate the source with the sample data located at demos/input.txt.

For instructions on how to setup a Kafka server and ingest data into it, refer to:
https://kafka.apache.org/082/documentation.html.
Once you have a Kafka server set up and have a 'test' topic created, you can ingest the sample input:

```
cat demos/input.txt | <path-to-kafka-install>/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```


## Step 7 - View the output

The status of the application can be monitored through the UI. Once the Spark application processes its
first micro batch of events, the output will be available in the specified sink:

```
cdap cli "execute 'select * from dataset_output'"
```


# CDAP Flow Implementation

Note that there is an alternate implementation of the ECA application which is executed using CDAP Flow, instead of
Apache Spark. To exercise this Flow, replace steps 5 through 7 with the following steps.

## Step 5 - Deploy and start a CDAP Flow

```
cdap cli deploy app eca-flow/target/eca-flow-1.2.0-SNAPSHOT.jar
cdap cli start flow eca.ECAFlow
```


## Step 6 - Ingest data in the source


```
cdap cli load stream eventStream demos/input.txt
```

## Step 7 - View the output

```
cdap cli "execute 'SELECT decode(value, \"UTF-8\") FROM default.dataset_events_actions'"
```


# Mailing Lists

CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

# License and Trademarks

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
