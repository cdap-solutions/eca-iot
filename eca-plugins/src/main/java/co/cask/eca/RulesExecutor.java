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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.eca.dataset.Rules;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("RulesExecutor")
public class RulesExecutor extends SparkCompute<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(RulesExecutor.class);

  private final Config config;

  public RulesExecutor(Config config) {
    this.config = config;
  }

  @Override
  public JavaRDD<StructuredRecord> transform(final SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    final SparkPipelineContext sparkWranglerPipelineContext = new SparkPipelineContext(sparkExecutionPluginContext);

    JavaPairRDD<byte[], Row> rulesRdd = sparkExecutionPluginContext.fromDataset("rules");
    JavaPairRDD<String, Row> rules = JavaPairRDD.fromJavaRDD(rulesRdd.map(new Function<Tuple2<byte[], Row>, Tuple2<String, Row>>() {
      @Override
      public Tuple2<String, Row> call(Tuple2<byte[], Row> rowTuple2) throws Exception {
        return new Tuple2<>(Bytes.toString(rowTuple2._1()), rowTuple2._2());
      }
    }));

    JavaPairRDD<String, StructuredRecord> hashedInput = javaRDD.keyBy(new Function<StructuredRecord, String>() {
      @Override
      public String call(StructuredRecord structuredRecord) throws Exception {
        return structuredRecord.get("key");
      }
    });

    JavaPairRDD<String, Iterable<Tuple2<StructuredRecord, Optional<Row>>>> joined = hashedInput.leftOuterJoin(rules).groupByKey();
    // filter out all the records for which rules are missing. TODO: deal with them later
    joined = joined.filter(new Function<Tuple2<String, Iterable<Tuple2<StructuredRecord, Optional<Row>>>>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, Iterable<Tuple2<StructuredRecord, Optional<Row>>>> integerIterableTuple2) throws Exception {
        Iterator<Tuple2<StructuredRecord, Optional<Row>>> tuples = integerIterableTuple2._2().iterator();
        Optional<Row> rowOptional = tuples.next()._2();
        return rowOptional.isPresent();
      }
    });

    return joined.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Tuple2<StructuredRecord,Optional<Row>>>>, StructuredRecord>() {
      @Override
      public Iterator<StructuredRecord> call(Tuple2<String, Iterable<Tuple2<StructuredRecord, Optional<Row>>>> integerIterableTuple2) throws Exception {
        Iterable<Tuple2<StructuredRecord, Optional<Row>>> tuples = integerIterableTuple2._2();
        Tuple2<StructuredRecord, Optional<Row>> tuple = tuples.iterator().next();
        // we already know the row is present (we filtered absent rows above)
        Rules rules = Rules.fromRow(tuple._2().get());

        List<Record> records = new ArrayList<>();
        Iterator<Tuple2<StructuredRecord, Optional<Row>>> iter = tuples.iterator();
        while (iter.hasNext()) {
          // concern of fitting all in memory. TODO: make wrangler pipeline executor streaming
          StructuredRecord structuredRecord = iter.next()._1();
          records.add(new Record("event", structuredRecord.get(config.eventField)));
        }

        PipelineExecutor pipeline = new PipelineExecutor();


        List<String> directives = Lists.newArrayList("parse-as-json event", "drop event", "columns-replace s/event_//");

        for (Rules.ActionableCondition condition : rules.getConditions()) {
          // set column sms age > 5
          directives.add(String.format("set column %s %s", condition.getActionType(), condition.getCondition()));
        }

        pipeline.configure(new TextDirectives(directives.toArray(new String[]{})),
                           sparkWranglerPipelineContext);
        // we clone the input records so we still have access to the unmodified input
        List<Record> outputRecords = pipeline.execute(cloneRecords(records));
        Preconditions.checkState(records.size() == outputRecords.size());

        Set<String> actionTypes = new HashSet<>();
        for (Rules.ActionableCondition actionableCondition : rules.getConditions()) {
          Preconditions.checkArgument(!actionTypes.contains(actionableCondition.getActionType()));
          actionTypes.add(actionableCondition.getActionType());
        }

        // execute conditions on all these and create output StructuredRecords
        List<StructuredRecord> outputStructuredRecords = new ArrayList<>();
        for (int i = 0; i < outputRecords.size(); i++) {
          Record outputRecord = records.get(i);
          Map<String, Boolean> conditionResults = new HashMap<>();
          for (String actionType : actionTypes) {
            conditionResults.put(actionType, (Boolean) outputRecords.get(i).getValue(actionType));
          }
          outputStructuredRecords.add(toStructuredRecord(outputRecord, conditionResults));
        }
        return outputStructuredRecords.iterator();
      }
    });
  }

  private List<Record> cloneRecords(List<Record> records) {
    List<Record> cloned = new ArrayList<>();
    for (Record record : records) {
      cloned.add(new Record(record));
    }
    return cloned;
  }

  private StructuredRecord toStructuredRecord(Record record, Map<String, Boolean> conditionResults) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of(config.eventField, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    for (String actionType : conditionResults.keySet()) {
      fields.add(Schema.Field.of(actionType, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
    }
    Schema schema = Schema.recordOf("event", fields);

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    builder.set(config.eventField, record.getValue("event"));
    for (Map.Entry<String, Boolean> entry : conditionResults.entrySet()) {
      builder.set(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  /**
   * Config for the plugin.
   */
  public static class Config extends PluginConfig {
    @Description("Name of the field containing the event.")
    private String eventField;
  }

  @VisibleForTesting
  static ETLPlugin getPlugin(String eventField) {
    Map<String, String> properties = new HashMap<>();
    properties.put("eventField", eventField);
    return new ETLPlugin("RulesExecutor", SparkCompute.PLUGIN_TYPE, properties, null);
  }
}
