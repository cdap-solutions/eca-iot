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
import co.cask.eca.dataset.SchemaMeta;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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
@Name("EventParser")
public class EventParser extends SparkCompute<StructuredRecord, StructuredRecord> {

  private final Config config;

  public EventParser(Config config) {
    this.config = config;
  }

  @Override
  public JavaRDD<StructuredRecord> transform(final SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> inputRDD) throws Exception {
    final SparkPipelineContext sparkWranglerPipelineContext = new SparkPipelineContext(sparkExecutionPluginContext);

    JavaPairRDD<byte[], Row> schemaTable = sparkExecutionPluginContext.fromDataset("schemas");
    JavaPairRDD<Integer, Row> hashedSchemas = JavaPairRDD.fromJavaRDD(schemaTable.map(new Function<Tuple2<byte[], Row>, Tuple2<Integer, Row>>() {
      @Override
      public Tuple2<Integer, Row> call(Tuple2<byte[], Row> rowTuple2) throws Exception {
        int hash = Bytes.toInt(rowTuple2._1());
        return new Tuple2<>(hash, rowTuple2._2());
      }
    }));

    // between RDD Functions, we use Map<String, Object> instead of Record, because we can not use classes defined
    // in the plugin there, due to the fact that the deserialization will not use PluginClassLoader, but MainClassLoader
    JavaPairRDD<Integer, Map<String, Object>> hashedInput = inputRDD.map(new Function<StructuredRecord, Record>() {
      @Override
      public Record call(StructuredRecord structuredRecord) throws Exception {
        Record record = new Record("event", structuredRecord.get(config.eventField));
        PipelineExecutor pipeline = new PipelineExecutor();
        // remove the 'event_' prefix from all the column names
        pipeline.configure(new TextDirectives(new String[] {"parse-as-json event", "drop event", "columns-replace s/event_//"}), sparkWranglerPipelineContext);

        // TODO: reuse the PipelineExecutor across records
        // we know the parse-as-json will only emit 1 output record, given 1 input record
        return pipeline.execute(ImmutableList.of(record)).get(0);
      }
    }).keyBy(new Function<Record, Integer>() {
      @Override
      public Integer call(Record record) throws Exception {

        Set<String> fieldNames = new HashSet<>();
        for (int i = 0; i < record.length(); i++) {
          fieldNames.add(record.getColumn(i));
        }
        return Rules.hashFields(fieldNames);
      }
    }).mapValues(new Function<Record, Map<String, Object>>() {
      @Override
      public Map<String, Object> call(Record record) throws Exception {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < record.length(); i++) {
          map.put(record.getColumn(i), record.getValue(i));
        }
        return map;
      }
    });


    // we perform leftOuterJoin, so that we know if any of the input records are missing corresponding schemas
    JavaPairRDD<Integer, Iterable<Tuple2<Map<String, Object>, Optional<Row>>>> joined = hashedInput.leftOuterJoin(hashedSchemas).groupByKey();
    // filter out all the records for which schemas are missing. TODO: deal with them later
    joined = joined.filter(new Function<Tuple2<Integer, Iterable<Tuple2<Map<String, Object>, Optional<Row>>>>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Integer, Iterable<Tuple2<Map<String, Object>, Optional<Row>>>> integerIterableTuple2) throws Exception {
        Iterator<Tuple2<Map<String, Object>, Optional<Row>>> tuples = integerIterableTuple2._2().iterator();
        Optional<Row> rowOptional = tuples.next()._2();
        return rowOptional.isPresent();
      }
    });


    return joined.flatMap(new FlatMapFunction<Tuple2<Integer,Iterable<Tuple2<Map<String, Object>, Optional<Row>>>>, StructuredRecord>() {
      @Override
      public Iterable<StructuredRecord> call(Tuple2<Integer, Iterable<Tuple2<Map<String, Object>, Optional<Row>>>> integerIterableTuple2) throws Exception {
        Iterable<Tuple2<Map<String, Object>, Optional<Row>>> tuples = integerIterableTuple2._2();
        Tuple2<Map<String, Object>, Optional<Row>> tuple = tuples.iterator().next();
        // we already know the row is present (we filtered absent rows above)
        Row row = tuple._2().get();
        List<String> userDirectives = SchemaMeta.fromRow(row).getSchema().getDirectives();

        List<Record> records = new ArrayList<>();
        Iterator<Tuple2<Map<String, Object>, Optional<Row>>> iter = tuples.iterator();
        while (iter.hasNext()) {
          records.add(fromMap(iter.next()._1()));
        }

        PipelineExecutor pipeline = new PipelineExecutor();
        List<String> directives = new ArrayList<>(userDirectives);
        directives.addAll(ImmutableList.of("write-as-json-map event", "keep event,key"));
        pipeline.configure(new TextDirectives(directives.toArray(new String[]{})),
                           sparkWranglerPipelineContext);

        List<Record> execute = pipeline.execute(records);

        List<StructuredRecord> structuredRecords = new ArrayList<>();
        for (Record record : execute) {
          structuredRecords.add(toStructuredRecord(record));
        }
        return structuredRecords;
      }
    });
  }

  private StructuredRecord toStructuredRecord(Record record) {
    Schema schema = Schema.recordOf("event",
                                    Schema.Field.of(config.eventField, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("key", Schema.of(Schema.Type.STRING)));

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    builder.set(config.eventField, record.getValue("event"));
    builder.set("key", record.getValue("key"));
    return builder.build();
  }

  private Record fromMap(Map<String, Object> map) {
    Record record = new Record();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      record.add(entry.getKey(), entry.getValue());
    }
    return record;
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
    return new ETLPlugin("EventParser", SparkCompute.PLUGIN_TYPE, properties, null);
  }
}
