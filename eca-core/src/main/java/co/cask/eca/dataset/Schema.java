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

import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class Schema {

  private final String name;
  private final String displayName;
  private final String description;
  private final Set<String> uniqueFieldNames;
  private final List<String> directives;


  public Schema(String name, Set<String> uniqueFieldNames) {
    // just use empty list of directives
    this(name, uniqueFieldNames, Collections.<String>emptyList());
  }

  public Schema(String name, Set<String> uniqueFieldNames, List<String> directives) {
    // just use the name as the displayName and description
    this(name, name, name, uniqueFieldNames, directives);
  }

  public Schema(String name, String displayName, String description, Set<String> uniqueFieldNames, List<String> directives) {
    this.name = name;
    this.displayName = displayName;
    this.description = description;
    this.uniqueFieldNames = uniqueFieldNames;
    this.directives = directives;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDescription() {
    return description;
  }

  public Set<String> getUniqueFieldNames() {
    return uniqueFieldNames;
  }

  public List<String> getDirectives() {
    return directives;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Schema that = (Schema) o;

    return Objects.equal(this.name, that.name) &&
      Objects.equal(this.displayName, that.displayName) &&
      Objects.equal(this.description, that.description) &&
      Objects.equal(this.uniqueFieldNames, that.uniqueFieldNames) &&
      Objects.equal(this.directives, that.directives);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, displayName, description, uniqueFieldNames, directives);
  }
}
