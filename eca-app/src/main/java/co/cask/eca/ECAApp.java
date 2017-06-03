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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.eca.service.SchemaHttpHandler;
import co.cask.eca.service.RulesHttpHandler;
import co.cask.eca.service.web.WebUIHandler;

/**
 * A CDAP Application consisting of two datasets: rules and schemas. A Service is included to interact with these
 * datasets.
 */
public class ECAApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("eca");
    setDescription("ECA is an application where events are parsed, conditions are applied on those events," +
                     " and actions are executed if those conditions are met.");
    addService("service", new SchemaHttpHandler(), new RulesHttpHandler(), new WebUIHandler());
    // we're not currently using the index on "n" for rules
    createDataset("rules", IndexedTable.class.getName(), DatasetProperties.builder().add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "n").build());
    createDataset("schemas", IndexedTable.class.getName(), DatasetProperties.builder().add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "n").build());
  }
}
