/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.eca.service.web;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Serves static web ui content.
 */
public class WebUIHandler extends AbstractHttpServiceHandler {

  @TransactionPolicy(TransactionControl.EXPLICIT)
  @Path("{file}")
  @GET
  public void serve(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("file") String file)
    throws IOException {

    InputStream resource = getClass().getResourceAsStream(file);
    if (resource == null) {
      responder.sendError(404, "Resource does not exist: " + file);
      return;
    }


    String contentType = "application/octet-stream";
    if (file.endsWith(".html")) {
      contentType = "text/html";
    } else if (file.endsWith(".png")) {
      contentType = "image/png";
    } else if (file.endsWith(".svg")) {
      contentType = "image/svg+xml";
    }

    ByteBuffer bb = ByteBuffer.wrap(ByteStreams.toByteArray(resource));
    responder.send(200, bb, contentType, new HashMap<String, String>());
  }
}
