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

import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.wrangler.api.PipelineContext;

import java.io.Serializable;
import java.util.Map;

/**
 * An implementation of {@link PipelineContext}, used in Spark plugins.
 */
public class SparkPipelineContext implements PipelineContext, Serializable {
  private static final long serialVersionUID = 6026151626011420395L;
  private final StageMetrics metrics;
  private final String stageName;
  private final Map<String, String> properties;

  SparkPipelineContext(SparkExecutionPluginContext sparkExecutionPluginContext) {
    metrics = sparkExecutionPluginContext.getMetrics();
    stageName = sparkExecutionPluginContext.getStageName();
    properties = sparkExecutionPluginContext.getPluginProperties().getProperties();
  }

  @Override
  public StageMetrics getMetrics() {
    return metrics;
  }

  @Override
  public String getContextName() {
    return stageName;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public <T> Lookup<T> provide(String s, Map<String, String> map) {
    throw new UnsupportedOperationException("Lookup is not supported in Spark pipelines.");
  }
}
