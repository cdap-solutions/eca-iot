/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.eca;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.IntegrationTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.eca.dataset.Rules;
import co.cask.eca.service.util.RulesManager;
import co.cask.eca.service.util.SchemaManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class EventParserIntegrationTest extends IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(EventParserIntegrationTest.class);

  private static final ArtifactSummary DATA_STREAM_ARTIFACT =
    new ArtifactSummary("cdap-data-streams", "4.0.0", ArtifactScope.SYSTEM);

  // TODO: need EventParser and RulesExecutor deployed as system artifacts
//  @Test
  public void testEventParser() throws Exception {
    ApplicationManager ecaApp = deployApplication(ECAApp.class);
    ServiceManager serviceManager = ecaApp.getServiceManager("service").start();
    URL baseURL = serviceManager.getServiceURL(60, TimeUnit.SECONDS);

    SchemaManager schemas = new SchemaManager(baseURL);
    RulesManager rules = new RulesManager(baseURL);

    ImmutableSet<String> fields = ImmutableSet.of("age", "name", "key");

    Rules.ActionableCondition ageOver14Condition = new Rules.ActionableCondition("age > 14", "sms");
    rules.create(new Rules("device002", ImmutableList.of(ageOver14Condition)));

    schemas.create(new co.cask.eca.dataset.Schema("someSchema", fields));

    Schema sourceSchema = Schema.recordOf(
      "event",
      Schema.Field.of("event", Schema.of(Schema.Type.STRING))
    );

    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("referenceName", "myStream");
    sourceProperties.put("name", "myStream");
    sourceProperties.put("format", "text");
    sourceProperties.put("schema", sourceSchema.toString());

    ETLPlugin source = new ETLPlugin("Stream", StreamingSource.PLUGIN_TYPE, sourceProperties, null);


    Schema sinkSchema = Schema.recordOf(
      "event",
      Schema.Field.of("event", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("sms", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
    ImmutableMap<String, String> sinkProperties = ImmutableMap.of(
      Properties.TimePartitionedFileSetDataset.SCHEMA, sinkSchema.toString(),
      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "output");
    ETLPlugin sink = new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE, sinkProperties, null);

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      // TODO: change this to use stream realtime source
      .addStage(new ETLStage("source", source))
      .addStage(new ETLStage("eventparser", EventParser.getPlugin("event")))
      .addStage(new ETLStage("rulesexecutor", RulesExecutor.getPlugin("event")))
      .addStage(new ETLStage("sink", sink))
      .addConnection("source", "eventparser")
      .addConnection("eventparser", "rulesexecutor")
      .addConnection("rulesexecutor", "sink")
//      .setBatchInterval("1s")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("rulesExecution");
    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(DATA_STREAM_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager("DataStreamsSparkStreaming").start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 60, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(5);

    StreamManager myStream = getTestManager().getStreamManager(NamespaceId.DEFAULT.stream("myStream"));
    myStream.createStream();
    String samEvent = "{name: 'samuel', age: 24, key: 'device002'}";
    myStream.send(samEvent);
    myStream.send("{name: 'bob', age: 13, key: 'device002'}");
    myStream.send("{name: 'jane', age: 14, key: 'device002'}");


    System.out.println("abcd0");
    LOG.info("abcd1");
    TimeUnit.SECONDS.sleep(1000);
//    DataSetManager<Table> outputTable = getDataset("outputTable");
//    Set<StructuredRecord> actual = new HashSet<>(MockSink.readOutput(outputTable));

//    Assert.assertEquals(3, actual.size());

    Map<String, Boolean> expectedMatches = ImmutableMap.of("jane", false,
                                                           "bob", false,
                                                           "samuel", true);
//    Assert.assertEquals(expectedMatches, matchedByName(actual));
//    Assert.assertEquals(samEvent, outputRecord.get("event"));

//    validateMetric(3, appId, "source.records.out");
//    validateMetric(3, appId, "sink.records.in");

//    deleteDatasetInstance(NamespaceId.DEFAULT.dataset("inputTable"));
//    deleteDatasetInstance(NamespaceId.DEFAULT.dataset("outputTable"));
  }

  private Map<String, Boolean> matchedByName(Set<StructuredRecord> records) {
    Map<String, Boolean> matchedByName = new HashMap<>();
    JsonParser jsonParser = new JsonParser();
    for (StructuredRecord record : records) {
      String name = jsonParser.parse((String) record.get("event")).getAsJsonObject().get("name").getAsString();
      Map<String, Boolean> conditionMatches = new Gson().fromJson((String) record.get("conditionResults"),
                                                                  new TypeToken<Map<String, Boolean>>() { }.getType());
      // currently, we only test rules for sms action type
      matchedByName.put(name, conditionMatches.get("sms"));
    }
    return matchedByName;
  }
}
