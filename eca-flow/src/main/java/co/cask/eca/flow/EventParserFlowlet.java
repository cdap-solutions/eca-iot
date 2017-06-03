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
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.eca.dataset.Rules;
import co.cask.eca.dataset.SchemaMeta;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A Flowlet that parses events by applying a set of Wrangler directives on them.
 */
public class EventParserFlowlet extends AbstractFlowlet {

  private IndexedTable schemas;

  private Metrics metrics;

  private OutputEmitter<ParsedEvent> out;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    schemas = context.getDataset("schemas");
  }

  @Batch(100)
  @ProcessInput
  public void process(StreamEvent event) throws PipelineException {
    FlowletPipelineContext flowletPipelineContext = new FlowletPipelineContext(getContext(), metrics);

    Record record = new Record("event", Bytes.toString(event.getBody()));
    PipelineExecutor pipeline = new PipelineExecutor();
    // remove the 'event_' prefix from all the column names
    pipeline.configure(new TextDirectives(new String[] {"parse-as-json event", "drop event", "columns-replace s/event_//"}), flowletPipelineContext);

    // TODO: reuse the PipelineExecutor across records
    // we know the parse-as-json will only emit 1 output record, given 1 input record
    Record parsedRecord = pipeline.execute(ImmutableList.of(record)).get(0);

    int hash = hashFieldNames(parsedRecord);

    Row row = schemas.get(Bytes.toBytes(hash));
    if (row.isEmpty()) {
      return;
    }

    List<String> userDirectives = SchemaMeta.fromRow(row).getSchema().getDirectives();

    // TODO: do we need to re-instantiate the pipeline executor?
    pipeline = new PipelineExecutor();
    List<String> directives = new ArrayList<>(userDirectives);
    directives.addAll(ImmutableList.of("write-as-json-map event", "keep event,key"));
    pipeline.configure(new TextDirectives(directives.toArray(new String[]{})),
                       flowletPipelineContext);

    List<Record> outputRecords = pipeline.execute(ImmutableList.of(parsedRecord));


    Record outputRecord = outputRecords.get(0);
    out.emit(new ParsedEvent((String) outputRecord.getValue("event"),
                             (String) outputRecord.getValue("key"),
                             hash),
             "eventHash", hash);
  }

  public static final class ParsedEvent {
    String event;
    String key;
    int hash;

    public ParsedEvent(String event, String key, int hash) {
      this.event = event;
      this.key = key;
      this.hash = hash;
    }
  }

  private int hashFieldNames(Record record) {
    Set<String> fieldNames = new HashSet<>();
    for (int i = 0; i < record.length(); i++) {
      fieldNames.add(record.getColumn(i));
    }
    return Rules.hashFields(fieldNames);
  }

}
