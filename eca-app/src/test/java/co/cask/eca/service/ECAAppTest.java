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

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.eca.ECAApp;
import co.cask.eca.dataset.Rules;
import co.cask.eca.dataset.Schema;
import co.cask.eca.dataset.SchemaMeta;
import co.cask.eca.service.util.RulesManager;
import co.cask.eca.service.util.SchemaManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URL;
import java.util.HashSet;
import java.util.List;

/**
 * Tests for {@link SchemaHttpHandler}.
 */
@Ignore
public class ECAAppTest extends TestBase {

  @Override
  public void afterTest() throws Exception {
    // super.afterTest stops all running programs
    super.afterTest();
    clear();
  }

  @Test
  public void testSchemas() throws Exception {
    ApplicationManager ecaApp = deployApplication(ECAApp.class);
    ServiceManager serviceManager = ecaApp.getServiceManager("service").start();
    URL baseURL = serviceManager.getServiceURL();

    SchemaManager schemas = new SchemaManager(baseURL);
    Assert.assertEquals(0, schemas.list().size());

    Schema schema1 = new Schema("schema1", ImmutableSet.of("age"));
    schemas.create(schema1);
    Assert.assertEquals(schema1, schemas.get(schema1.getName()));

    List<SchemaMeta> list = schemas.list();
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(schema1, list.get(0).getSchema());

    Schema schema2 = new Schema("schema2", ImmutableSet.of("age", "gender"));
    schemas.create(schema2);
    Assert.assertEquals(schema2, schemas.get(schema2.getName()));

    list = schemas.list();
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(ImmutableSet.of(schema1, schema2),
                        ImmutableSet.of(list.get(0).getSchema(), list.get(1).getSchema()));

    schemas.delete(schema1.getName());
    schemas.delete(schema2.getName());
    Assert.assertEquals(0, schemas.list().size());

//    schemas.get(baseURL, schema2.getName());
//    schemas.delete(baseURL, schema2.getName());
  }


  @Test
  public void testRules() throws Exception {
    ApplicationManager ecaApp = deployApplication(ECAApp.class);
    ServiceManager serviceManager = ecaApp.getServiceManager("service").start();
    URL baseURL = serviceManager.getServiceURL();

    RulesManager rules = new RulesManager(baseURL);

    Assert.assertEquals(0, rules.list().size());

    Rules rules1 = new Rules("device001", ImmutableList.of(new Rules.ActionableCondition("age > 4", "sms")));
    rules.create(rules1);
//    Assert.assertEquals(rules1, rules.get(baseURL, Rules.hashFields(rules1.getFields())));

    Assert.assertEquals(ImmutableSet.of(rules1), new HashSet<>(rules.list()));

    Rules rules2 = new Rules("device002", ImmutableList.of(new Rules.ActionableCondition("gender == 'm'", "sms")));
    rules.create(rules2);
//    Assert.assertEquals(rules2, rules.get(baseURL, Rules.hashFields(rules2.getFields())));
    Assert.assertEquals(ImmutableSet.of(rules1, rules2), new HashSet<>(rules.list()));

    rules.delete(rules1.getName());
    rules.delete(rules2.getName());
    Assert.assertEquals(0, rules.list().size());

//    rules.get(baseURL, schema2.getName());
//    rules.delete(baseURL, schema2.getName());
  }

  @Test
  public void testCreateAlreadyExists() throws Exception {
    ApplicationManager ecaApp = deployApplication(ECAApp.class);
    ServiceManager serviceManager = ecaApp.getServiceManager("service").start();
    URL baseURL = serviceManager.getServiceURL();

    RulesManager rules = new RulesManager(baseURL);

    Rules rules1 = new Rules("device001", ImmutableList.of(new Rules.ActionableCondition("age > 4", "sms")));
    rules.create(rules1);
    Rules rules2 = new Rules("device001", ImmutableList.of(new Rules.ActionableCondition("age > 4", "sms")));
    try {
      rules.create(rules2);
      Assert.fail();
    } catch (Exception e) {
      assertContains(e.getMessage(), "Rule with name 'device001' already exists.");
    }

    SchemaManager schemas = new SchemaManager(baseURL);
    Assert.assertEquals(0, schemas.list().size());

    Schema schema1 = new Schema("schema1", ImmutableSet.of("age"));
    schemas.create(schema1);

    Schema schema2 = new Schema("schema1", ImmutableSet.of("otherField"));
    try {
      schemas.create(schema2);
      Assert.fail();
    } catch (Exception e) {
      assertContains(e.getMessage(), "Schema with name 'schema1' already exists.");
    }

    Schema schema3 = new Schema("schema2", ImmutableSet.of("age"));
    try {
      schemas.create(schema3);
      Assert.fail();
    } catch (Exception e) {
      assertContains(e.getMessage(), String.format("Schema with hash '%s' already exists.",
                                                   Rules.hashFields(schema1.getUniqueFieldNames())));
    }
  }

  private void assertContains(String haystack, String needle) {
    Assert.assertTrue(String.format("Failed to find needle '%s' in haystack '%s'.", needle, haystack),
                      haystack.contains(needle));
  }
}
