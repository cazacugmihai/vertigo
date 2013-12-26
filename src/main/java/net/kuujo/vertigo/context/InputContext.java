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
package net.kuujo.vertigo.context;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonIgnore;

import net.kuujo.vertigo.component.Component;
import net.kuujo.vertigo.input.grouping.Grouping;
import net.kuujo.vertigo.serializer.Serializable;
import net.kuujo.vertigo.serializer.Serializer;
import net.kuujo.vertigo.serializer.SerializerFactory;

/**
 * Immutable component input.
 *
 * @author Jordan Halterman
 */
public final class InputContext implements Serializable {
  private @JsonIgnore ComponentContext<?> component;
  private String id;
  private String stream;
  private String address;
  private Grouping grouping;

  private InputContext() {
  }

  /**
   * Creates a new input context from JSON.
   *
   * @param context
   *   A JSON representation of the input context.
   * @return
   *   A new input context instance.
   * @throws MalformedContextException
   *   If the JSON context is malformed.
   */
  public static <T extends Component<T>> InputContext fromJson(JsonObject context) {
    Serializer<InputContext> serializer = SerializerFactory.getSerializer(InputContext.class);
    InputContext instance = serializer.deserialize(context.getObject("instance"));
    ComponentContext<T> component = ComponentContext.fromJson(context);
    return instance.setComponentContext(component);
  }

  /**
   * Serializes an input context to JSON.
   *
   * @param context
   *   The input context to serialize.
   * @return
   *   A Json representation of the input context.
   */
  public static JsonObject toJson(InputContext context) {
    Serializer<InputContext> serializer = SerializerFactory.getSerializer(InputContext.class);
    JsonObject json = ComponentContext.toJson(context.componentContext());
    return json.putObject("instance", serializer.serialize(context));
  }

  InputContext setComponentContext(ComponentContext<?> component) {
    this.component = component;
    return this;
  }

  /**
   * Gets the unique input ID.
   *
   * @return
   *   The input ID.
   */
  public String id() {
    return id;
  }

  /**
   * Returns the input address.
   *
   * @return
   *   The input address.
   */
  public String address() {
    return address;
  }

  /**
   * Returns the input stream.
   *
   * @return
   *   The input stream.
   */
  public String stream() {
    return stream;
  }

  /**
   * Returns the input grouping.
   *
   * @return
   *   The input grouping.
   */
  public Grouping grouping() {
    return grouping;
  }

  /**
   * Returns the input count.
   *
   * @return
   *   The input count.
   */
  public int count() {
    return component != null ? component.numInstances() : 1;
  }

  /**
   * Returns the parent component context.
   *
   * @return
   *   The parent component context.
   */
  public ComponentContext<?> componentContext() {
    return component;
  }

}
