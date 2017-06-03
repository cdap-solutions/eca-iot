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

package co.cask.eca.service.util;

import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.eca.dataset.Schema;
import co.cask.eca.dataset.SchemaMeta;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 *
 */
// doesn't work with authentication
public final class SchemaManager extends DataManager {
  private static final Gson GSON = new Gson();
  private final URL baseURL;

  public SchemaManager(URL baseURL) {
    this.baseURL = baseURL;
  }

  public void create(Schema schema) throws IOException {
    HttpRequest request = HttpRequest.put(new URL(baseURL, "schemas/" + schema.getName())).withBody(GSON.toJson(schema)).build();
    doRequest(request);
  }

  public void delete(String name) throws IOException {
    doRequest(HttpRequest.delete(new URL(baseURL, "schemas/" + name)).build());
  }

  public Schema get(String name) throws IOException {
    HttpResponse response = doRequest(HttpRequest.get(new URL(baseURL, "schemas/" + name)).build());
    return GSON.fromJson(response.getResponseBodyAsString(), SchemaMeta.class).getSchema();
  }

  public List<SchemaMeta> list() throws IOException {
    HttpResponse response = doRequest(HttpRequest.get(new URL(baseURL, "schemas")).build());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<List<SchemaMeta>>() { }.getType());
  }

}
