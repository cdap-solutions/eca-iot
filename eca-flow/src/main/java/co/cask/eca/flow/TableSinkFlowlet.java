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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.wrangler.api.PipelineException;
import com.google.gson.Gson;

/**
 * A Flowlet that simply writes the events to a table, keyed by the hash of the event String.
 */
public class TableSinkFlowlet extends AbstractFlowlet {

  private static final Gson GSON = new Gson();

  private KeyValueTable eventsActions;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    eventsActions = context.getDataset(ECAFlowApp.EVENT_ACTIONS);
  }

  @ProcessInput
  public void process(RulesExecutorFlowlet.EventWithAction event) throws PipelineException {
    eventsActions.write(Bytes.toBytes(event.event.hashCode()), Bytes.toBytes(GSON.toJson(event)));
  }
}
