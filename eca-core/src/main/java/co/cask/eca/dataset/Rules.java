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

package co.cask.eca.dataset;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import com.google.common.base.Objects;
import com.google.gson.Gson;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class Rules implements Serializable {
  private static final long serialVersionUID = -6977012428463731561L;

  private static final byte[] CONTENTS_COL = new byte[]{'c'};

  private final String name;
  private final List<ActionableCondition> conditions;

  public Rules(String name, List<ActionableCondition> conditions) {
    this.name = name;
    this.conditions = conditions;
  }

  // TODO: move to helper class?
  public static Put toPut(Rules rules) {
    return new Put(Bytes.toBytes(rules.getName()))
      .add(CONTENTS_COL, new Gson().toJson(rules));
  }

  public static Rules fromRow(Row row) {
    byte[] bytes = row.get(CONTENTS_COL);
    return new Gson().fromJson(Bytes.toString(bytes), Rules.class);
  }

  // TODO: directive-ize the hashing
  public static int hashFields(Set<String> fields) {
    List<String> fieldsList = new ArrayList<>(fields);
    Collections.sort(fieldsList);
    return fieldsList.hashCode();
  }

  public String getName() {
    return name;
  }

  public List<ActionableCondition> getConditions() {
    return conditions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Rules that = (Rules) o;

    return Objects.equal(this.name, that.name) &&
      Objects.equal(this.conditions, that.conditions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, conditions);
  }

  public static final class ActionableCondition implements Serializable {
    private static final long serialVersionUID = -498621181043048485L;
    private final String condition;
    private final String actionType;

    public ActionableCondition(String condition, String actionType) {
      this.condition = condition;
      this.actionType = actionType;
    }

    public String getCondition() {
      return condition;
    }

    public String getActionType() {
      return actionType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ActionableCondition that = (ActionableCondition) o;

      return Objects.equal(this.condition, that.condition) &&
        Objects.equal(this.actionType, that.actionType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(condition, actionType);
    }
  }

}
