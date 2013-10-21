/*
* Copyright 2013 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package net.kuujo.vertigo.input;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.vertigo.filter.Filter;
import net.kuujo.vertigo.selector.Selector;
import net.kuujo.vertigo.serializer.Serializable;
import net.kuujo.vertigo.serializer.Serializer;

/**
 * A component input.
 *
 * @author Jordan Halterman
 */
public class Input implements Serializable {

  public static final String ID = "id";
  public static final String ADDRESS = "address";
  public static final String SELECTOR = "selector";
  public static final String FILTERS = "filters";

  private JsonObject definition;

  public Input(String address) {
    definition = new JsonObject().putString(ADDRESS, address);
  }

  /**
   * Returns the input ID.
   *
   * Input IDs may be shared across multiple instances of an input
   * and are the mechanism by which inputs are grouped together.
   *
   * @return
   *   The input identifier.
   */
  public String id() {
    return definition.getString(ID);
  }

  /**
   * Returns the input address.
   *
   * @return
   *   The input address.
   */
  public String getAddress() {
    return definition.getString(ADDRESS);
  }

  /**
   * Sets the input selector.
   *
   * @param selector
   *   An input selector.
   * @return
   *   The called input instance.
   */
  public Input deliverBy(Selector selector) {
    definition.putObject(SELECTOR, Serializer.serialize(selector));
    return this;
  }

  /**
   * Adds an input filter.
   *
   * @param filter
   *   An input filter.
   * @return
   *   The called input instance.
   */
  public Input filterBy(Filter filter) {
    JsonArray filters = definition.getArray(FILTERS);
    if (filters == null) {
      filters = new JsonArray();
      definition.putArray(FILTERS, filters);
    }
    filters.add(Serializer.serialize(filter));
    return this;
  }

  @Override
  public JsonObject getState() {
    return definition;
  }

  @Override
  public void setState(JsonObject state) {
    definition = state;
  }

}
