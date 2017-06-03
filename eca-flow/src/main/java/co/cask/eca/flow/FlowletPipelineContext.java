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

import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.wrangler.api.PipelineContext;

import java.util.Map;

/**
 * An implementation of {@link PipelineContext}, used in Flowlets.
 */
public class FlowletPipelineContext implements PipelineContext {

  private final FlowletContext flowletContext;
  private final DefaultStageMetrics stageMetrics;
  private final DatasetContextLookupProvider lookupProvider;

  public FlowletPipelineContext(FlowletContext flowletContext, Metrics metrics) {
    this.flowletContext = flowletContext;
    this.stageMetrics = new DefaultStageMetrics(metrics, flowletContext.getName());
    this.lookupProvider = new DatasetContextLookupProvider(flowletContext);
  }

  @Override
  public StageMetrics getMetrics() {
    return stageMetrics;
  }

  @Override
  public String getContextName() {
    return flowletContext.getName();
  }

  @Override
  public Map<String, String> getProperties() {
    return flowletContext.getRuntimeArguments();
  }

  @Override
  public <T> Lookup<T> provide(String s, Map<String, String> map) {
    return lookupProvider.provide(s, map);
  }
}
