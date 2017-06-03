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

import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * Contains common functionality for interacting with services.
 */
public class DataManager {

  protected HttpResponse doRequest(HttpRequest request) throws IOException {
    HttpResponse response = HttpRequests.execute(request);
    Preconditions.checkState(200 == response.getResponseCode(),
                             "Expected 200, but got response code '%s', response body '%s'.",
                             response.getResponseCode(), response.getResponseBodyAsString());
    return response;
  }
}
