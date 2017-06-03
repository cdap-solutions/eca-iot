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

package co.cask.eca.service;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.eca.dataset.Schema;
import co.cask.eca.dataset.SchemaMeta;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * HttpHandler for interacting with the schemas dataset.
 */
@Path("/schemas")
public class SchemaHttpHandler extends AbstractHttpServiceHandler {

  private static final Gson GSON = new Gson();

  private IndexedTable schemas;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    schemas = context.getDataset("schemas");
  }

  /**
   * Creates a schema.
   */
  @PUT
  @Path("{name}")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("name") String name) {
    String encodedSchema = new String(ChannelBuffers.copiedBuffer(request.getContent()).array());
    if (Strings.isNullOrEmpty(encodedSchema)) {
      responder.sendError(400, "No body found in http request.");
      return;
    }
    Schema schema;
    try {
      schema = GSON.fromJson(encodedSchema, Schema.class);
    } catch (JsonSyntaxException e) {
      responder.sendError(400, "Failed to parse Schema. " + e.getMessage());
      return;
    }

    if (schema.getUniqueFieldNames() == null || schema.getUniqueFieldNames().isEmpty()) {
      // only 'name' and 'uniqueFieldNames' are required
      responder.sendError(400, "Missing field 'uniqueFieldNames'.");
      return;
    }
    schema = new Schema(Objects.firstNonNull(schema.getName(), name),
                        Objects.firstNonNull(schema.getDisplayName(), name),
                        Objects.firstNonNull(schema.getDescription(), name),
                        schema.getUniqueFieldNames(),
                        Objects.firstNonNull(schema.getDirectives(), Collections.<String>emptyList()));

    if (schema.getName().isEmpty()) {
      responder.sendError(400, "Empty 'name' field is not allowed.");
      return;
    }

    Put put = SchemaMeta.toPut(new SchemaMeta(schema, System.currentTimeMillis()));

    if (!schemas.get(put.getRow()).isEmpty()) {
      responder.sendError(400, String.format("Schema with hash '%s' already exists.", Bytes.toInt(put.getRow())));
      return;
    }
    Row row = get(schema.getName(), null);
    if (row != null) {
      responder.sendError(400, String.format("Schema with name '%s' already exists.", schema.getName()));
      return;
    }
    schemas.put(put);

    // TODO: consolidate with update?
    responder.sendStatus(200);
  }

  /**
   * Deletes a particular schema.
   */
  @DELETE
  @Path("{name}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("name") String name) {
    Row row = get(name, responder);
    if (row == null) {
      return;
    }
    schemas.delete(row.getRow());
    responder.sendStatus(200);
  }

  /**
   * Gets a particular schema.
   */
  @GET
  @Path("{name}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("name") String name) {
    Row row = get(name, responder);
    if (row == null) {
      return;
    }
    responder.sendJson(SchemaMeta.fromRow(row));
  }

  // returns null if the given name doesn't exist, in which case responder is responded to with 404
  // if the responder is nonnull
  @Nullable
  private Row get(String name, @Nullable HttpServiceResponder responder) {
    byte[] nameBytes = Bytes.toBytes(name);
    Scanner scanner = schemas.readByIndex(SchemaMeta.NAME_COL, nameBytes);
    Row next = scanner.next();
    if (next == null) {
      if (responder != null) {
        responder.sendStatus(404);
      }
      return null;
    }
    // this shouldn't fail due to the check we have in create, that a Schema with the same name doesn't already exist
    Preconditions.checkState(scanner.next() == null);
    return next;
  }

  /**
   * Lists all schemas.
   */
  @GET
  @Path("")
  public void list(HttpServiceRequest request, HttpServiceResponder responder) {
    Scanner scanner = schemas.scan(null, null);
    List<SchemaMeta> schemas = new ArrayList<>();
    Row row;
    while ((row = scanner.next()) != null) {
      schemas.add(SchemaMeta.fromRow(row));
    }
    responder.sendJson(schemas);
  }
}
