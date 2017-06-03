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

package co.cask.eca.flow;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.eca.dataset.Rules;
import co.cask.eca.service.util.RulesManager;
import co.cask.eca.service.util.SchemaManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link ECAFlow}.
 */
public class ECAFlowTest extends TestBase {

  private static final Gson GSON = new Gson();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", "false");

  @Test
  public void testECAFlow() throws Exception {
    ApplicationManager ecaApp = deployApplication(ECAFlowApp.class);
    ServiceManager serviceManager = ecaApp.getServiceManager("service").start();
    URL baseURL = serviceManager.getServiceURL();

    SchemaManager schemas = new SchemaManager(baseURL);
    RulesManager rules = new RulesManager(baseURL);

    ImmutableSet<String> fields = ImmutableSet.of("age", "name", "key");

    Rules.ActionableCondition ageOver14Condition = new Rules.ActionableCondition("age > 14", "sms");
    rules.create(new Rules("device002", ImmutableList.of(ageOver14Condition)));

    schemas.create(new co.cask.eca.dataset.Schema("someSchema", fields));

    String samEvent = "{name: 'samuel', age: 24, key: 'device002'}";
    String bobEvent = "{name: 'bob', age: 13, key: 'device002'}";
    String janeEvent = "{name: 'jane', age: 14, key: 'device002'}";

    FlowManager flowManager = ecaApp.getFlowManager("ECAFlow").start();

    StreamManager inputStream = getStreamManager("eventStream");
    inputStream.send(samEvent);
    inputStream.send(bobEvent);
    inputStream.send(janeEvent);

    flowManager.getFlowletMetrics(TableSinkFlowlet.class.getSimpleName()).waitForProcessed(3, 10, TimeUnit.SECONDS);

    DataSetManager<KeyValueTable> eventsActionsTable = getDataset(ECAFlowApp.EVENT_ACTIONS);
    List<RulesExecutorFlowlet.EventWithAction> eventActions = new ArrayList<>();
    CloseableIterator<KeyValue<byte[], byte[]>> scan = eventsActionsTable.get().scan(null, null);
    while (scan.hasNext()) {
      KeyValue<byte[], byte[]> keyValue = scan.next();
      eventActions.add(GSON.fromJson(Bytes.toString(keyValue.getValue()),
                                          RulesExecutorFlowlet.EventWithAction.class));
    }

    Assert.assertEquals(3, eventActions.size());

    Map<String, Boolean> expectedMatches = ImmutableMap.of("jane", false,
                                                           "bob", false,
                                                           "samuel", true);
    Assert.assertEquals(expectedMatches, matchedByName(eventActions));

    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(ECAFlowApp.EVENT_ACTIONS));
  }

  private Map<String, Boolean> matchedByName(List<RulesExecutorFlowlet.EventWithAction> eventWithActions) {
    Map<String, Boolean> matchedByName = new HashMap<>();
    for (RulesExecutorFlowlet.EventWithAction eventWithAction : eventWithActions) {
      String name = new JsonParser().parse(eventWithAction.event).getAsJsonObject().get("name").getAsString();
      matchedByName.put(name, eventWithAction.conditionsResults.get("sms"));
    }
    return matchedByName;
  }
}
