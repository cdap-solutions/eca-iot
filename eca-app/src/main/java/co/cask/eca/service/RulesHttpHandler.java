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
import co.cask.eca.dataset.Rules;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * HttpHandler for interacting with the rules dataset.
 */
@Path("/rules")
public class RulesHttpHandler extends AbstractHttpServiceHandler {

  private static final Gson GSON = new Gson();

  private IndexedTable rules;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    rules = context.getDataset("rules");
  }

  /**
   * Creates a Rule.
   */
  @PUT
  @Path("{name}")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("name") String name) {
    String encodedRules = new String(ChannelBuffers.copiedBuffer(request.getContent()).array());
    if (Strings.isNullOrEmpty(encodedRules)) {
      responder.sendError(400, "No body found in http request.");
      return;
    }

    Rules rule;
    try {
      rule = GSON.fromJson(encodedRules, Rules.class);
    } catch (JsonSyntaxException e) {
      responder.sendError(400, "Failed to parse Rules. " + e.getMessage());
      return;
    }

    if (rule.getConditions() == null) {
      // only 'name' and 'conditions' are required
      responder.sendError(400, "Missing field 'conditions'");
      return;
    }

    rule = new Rules(Objects.firstNonNull(rule.getName(), name), rule.getConditions());
    if (rule.getName().isEmpty()) {
      responder.sendError(400, "Empty 'name' field is not allowed.");
      return;
    }

    // make sure people dont define an empty actionType or condition
    for (Rules.ActionableCondition actionableCondition : rule.getConditions()) {
      if (Strings.isNullOrEmpty(actionableCondition.getActionType())) {
        responder.sendError(400, "Missing 'actionType' field from condition.");
        return;
      }
      if (Strings.isNullOrEmpty(actionableCondition.getCondition())) {
        responder.sendError(400, "Missing 'condition' field from condition.");
        return;
      }
    }

    Put put = Rules.toPut(rule);

    if (!rules.get(put.getRow()).isEmpty()) {
      responder.sendError(400, String.format("Rule with name '%s' already exists.", rule.getName()));
      return;
    }
    rules.put(put);

    responder.sendStatus(200);
  }

  /**
   * Deletes a particular rule.
   */
  @DELETE
  @Path("{name}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("name") String name) {
    Row row = rules.get(Bytes.toBytes(name));
    if (row.isEmpty()) {
      responder.sendStatus(404);
      return;
    }
    rules.delete(row.getRow());
    responder.sendStatus(200);
  }

  /**
   * Lists all rules.
   */
  @GET
  @Path("")
  public void list(HttpServiceRequest request, HttpServiceResponder responder) {
    Scanner scanner = rules.scan(null, null);
    List<Rules> rules = new ArrayList<>();
    Row row;
    while ((row = scanner.next()) != null) {
      rules.add(Rules.fromRow(row));
    }
    responder.sendJson(rules);
  }
}
