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

package co.cask.eca.dataset;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import com.google.common.base.Objects;
import com.google.gson.Gson;

/**
 *
 */
public class SchemaMeta {
  public static final byte[] NAME_COL = new byte[]{'n'};
  private static final byte[] CONTENTS_COL = new byte[]{'c'};

  private final Schema schema;
  private final long created;
  private long lastModified;

  public SchemaMeta(Schema schema, long created) {
    this.schema = schema;
    this.created = created;
    this.lastModified = created;
  }

  // TODO: move to helper class?
  public static Put toPut(SchemaMeta schemaMeta) {
    Schema schema = schemaMeta.getSchema();
    return new Put(Bytes.toBytes(Rules.hashFields(schema.getUniqueFieldNames())))
      .add(CONTENTS_COL, new Gson().toJson(schemaMeta))
      .add(NAME_COL, schema.getName());
  }

  public static SchemaMeta fromRow(Row row) {
    byte[] bytes = row.get(CONTENTS_COL);
    return new Gson().fromJson(Bytes.toString(bytes), SchemaMeta.class);
  }

  public Schema getSchema() {
    return schema;
  }

  public long getCreated() {
    return created;
  }

  public long getLastModified() {
    return lastModified;
  }

  public void setLastModified(long lastModified) {
    this.lastModified = lastModified;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaMeta that = (SchemaMeta) o;

    return Objects.equal(this.schema, that.schema) &&
      Objects.equal(this.created, that.created) &&
      Objects.equal(this.lastModified, that.lastModified);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schema, created, lastModified);
  }
}
