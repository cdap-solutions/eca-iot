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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.eca.dataset.Rules;
import co.cask.eca.service.util.RulesManager;
import co.cask.eca.service.util.SchemaManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class EventParserTest extends HydratorTestBase {

  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary(APP_ARTIFACT_ID.getArtifact(),
                                                                          APP_ARTIFACT_ID.getVersion());

  private static int startCount = 0;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);

    // add some test plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      EventParser.class, RulesExecutor.class);
  }

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", "false");

  @Test
  public void testEventParser() throws Exception {
    ApplicationManager ecaApp = deployApplication(ECAApp.class);
    ServiceManager serviceManager = ecaApp.getServiceManager("service").start();
    URL baseURL = serviceManager.getServiceURL();

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

    String samEvent = "{name: 'samuel', age: 24, key: 'device002'}";
    StructuredRecord recordSamuel = StructuredRecord.builder(sourceSchema).set("event", samEvent).build();
    StructuredRecord recordBob = StructuredRecord.builder(sourceSchema).set("event", "{name: 'bob', age: 13, key: 'device002'}").build();
    StructuredRecord recordJane = StructuredRecord.builder(sourceSchema).set("event", "{name: 'jane', age: 14, key: 'device002'}").build();

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("inputTable", sourceSchema)))
      .addStage(new ETLStage("eventparser", EventParser.getPlugin("event")))
      .addStage(new ETLStage("rulesexecutor", RulesExecutor.getPlugin("event")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("outputTable")))
      .addConnection("source", "eventparser")
      .addConnection("eventparser", "rulesexecutor")
      .addConnection("rulesexecutor", "sink")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testTableLookup");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputTable = getDataset("inputTable");
    MockSource.writeInput(inputTable, ImmutableList.of(recordSamuel, recordBob, recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME).start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputTable = getDataset("outputTable");
    Set<StructuredRecord> actual = new HashSet<>(MockSink.readOutput(outputTable));

    Assert.assertEquals(3, actual.size());

    Map<String, Boolean> expectedMatches = ImmutableMap.of("jane", false,
                                                           "bob", false,
                                                           "samuel", true);
    Assert.assertEquals(expectedMatches, matchedByName(actual));
//    Assert.assertEquals(samEvent, outputRecord.get("event"));

//    validateMetric(3, appId, "source.records.out");
//    validateMetric(3, appId, "sink.records.in");

    deleteDatasetInstance(NamespaceId.DEFAULT.dataset("inputTable"));
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset("outputTable"));
  }

  private Map<String, Boolean> matchedByName(Set<StructuredRecord> records) {
    Map<String, Boolean> matchedByName = new HashMap<>();
    JsonParser jsonParser = new JsonParser();
    for (StructuredRecord record : records) {
      String name = jsonParser.parse((String) record.get("event")).getAsJsonObject().get("name").getAsString();
      Boolean value = record.get("sms");
      matchedByName.put(name, value);
    }
    return matchedByName;
  }
}
