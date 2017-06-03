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

import co.cask.cdap.api.flow.AbstractFlow;

/**
 * A {@link co.cask.cdap.api.flow.Flow} which parses events and executes conditions against them.
 */
public class ECAFlow extends AbstractFlow {

  @Override
  protected void configure() {
    EventParserFlowlet eventParserFlowlet = new EventParserFlowlet();
    RulesExecutorFlowlet rulesExecutorFlowlet = new RulesExecutorFlowlet();
    TableSinkFlowlet tableSinkFlowlet = new TableSinkFlowlet();
    addFlowlet(eventParserFlowlet);
    addFlowlet(rulesExecutorFlowlet);
    addFlowlet(tableSinkFlowlet);
    connectStream("eventStream", eventParserFlowlet);
    connect(eventParserFlowlet, rulesExecutorFlowlet);
    connect(rulesExecutorFlowlet, tableSinkFlowlet);
  }
}
