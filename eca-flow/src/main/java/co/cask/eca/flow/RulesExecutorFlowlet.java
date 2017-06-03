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

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.eca.dataset.Rules;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Flowlet that applies rules onto events and emits the results of these rules alongside the events.
 */
public class RulesExecutorFlowlet extends AbstractFlowlet {

  private IndexedTable rules;

  private Metrics metrics;

  private OutputEmitter<EventWithAction> out;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    rules = context.getDataset("rules");
  }

  @Batch(100)
  @HashPartition("eventHash")
  @ProcessInput
  public void process(EventParserFlowlet.ParsedEvent event) throws PipelineException {
    FlowletPipelineContext flowletPipelineContext = new FlowletPipelineContext(getContext(), metrics);

    Row row = rules.get(Bytes.toBytes(event.key));
    if (row.isEmpty()) {
      return;
    }
    Rules rules = Rules.fromRow(row);

    List<Record> records = Collections.singletonList(new Record("event", event.event));
    PipelineExecutor pipeline = new PipelineExecutor();
    List<String> directives = Lists.newArrayList("parse-as-json event", "drop event", "columns-replace s/event_//");

    for (Rules.ActionableCondition condition : rules.getConditions()) {
      // set column sms age > 5
      directives.add(String.format("set column %s %s", condition.getActionType(), condition.getCondition()));
    }

    pipeline.configure(new TextDirectives(directives.toArray(new String[]{})),
                       flowletPipelineContext);
    // we clone the input records so we still have access to the unmodified input
    List<Record> outputRecords = pipeline.execute(cloneRecords(records));
    Preconditions.checkState(records.size() == outputRecords.size());

    Set<String> actionTypes = new HashSet<>();
    for (Rules.ActionableCondition actionableCondition : rules.getConditions()) {
      Preconditions.checkArgument(!actionTypes.contains(actionableCondition.getActionType()));
      actionTypes.add(actionableCondition.getActionType());
    }

    // execute conditions on all these and emit as output
    for (int i = 0; i < outputRecords.size(); i++) {
      Record outputRecord = records.get(i);
      Map<String, Boolean> conditionResults = new HashMap<>();
      for (String actionType : actionTypes) {
        conditionResults.put(actionType, (Boolean) outputRecords.get(i).getValue(actionType));
      }
      out.emit(new EventWithAction((String) outputRecord.getValue("event"), event.hash, conditionResults));
    }
  }

  public static final class EventWithAction {
    String event;
    int hash;
    Map<String, Boolean> conditionsResults;

    public EventWithAction(String event, int hash, Map<String, Boolean> conditionsResults) {
      this.event = event;
      this.hash = hash;
      this.conditionsResults = conditionsResults;
    }
  }

  private List<Record> cloneRecords(List<Record> records) {
    List<Record> cloned = new ArrayList<>();
    for (Record record : records) {
      cloned.add(new Record(record));
    }
    return cloned;
  }

}
