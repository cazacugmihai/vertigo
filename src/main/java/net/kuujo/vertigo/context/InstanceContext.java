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

import net.kuujo.vertigo.serializer.Serializable;
import net.kuujo.vertigo.serializer.Serializer;
import net.kuujo.vertigo.serializer.SerializerFactory;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A component instance context which contains information regarding
 * a specific component (module or verticle) instance within a network.
 *
 * @author Jordan Halterman
 */
public final class InstanceContext implements Serializable {
  private String id;
  private int number;
  private @JsonIgnore ComponentContext<?> component;

  private InstanceContext() {
  }

  /**
   * Creates a new instance context from JSON.
   *
   * @param context
   *   A JSON representation of the instance context.
   * @return
   *   A new instance context instance.
   * @throws MalformedContextException
   *   If the JSON context is malformed.
   */
  public static InstanceContext fromJson(JsonObject context) {
    Serializer serializer = SerializerFactory.getSerializer(InstanceContext.class);
    InstanceContext instance = serializer.deserialize(context.getObject("instance"), InstanceContext.class);
    ComponentContext<?> component = ComponentContext.fromJson(context);
    return instance.setComponentContext(component);
  }

  /**
   * Serializes an instance context to JSON.
   *
   * @param context
   *   The instance context to serialize.
   * @return
   *   A Json representation of the instance context.
   */
  public static JsonObject toJson(InstanceContext context) {
    Serializer serializer = SerializerFactory.getSerializer(InstanceContext.class);
    JsonObject json = ComponentContext.toJson(context.componentContext().isModule() ?
        context.<ModuleContext>componentContext() : context.<VerticleContext>componentContext());
    return json.putObject("instance", serializer.serialize(context));
  }

  /**
   * Sets the instance parent.
   */
  InstanceContext setComponentContext(ComponentContext<?> component) {
    this.component = component;
    return this;
  }

  /**
   * Returns the instance ID.
   *
   * @return
   *   The instance ID.
   */
  public String id() {
    return id;
  }

  /**
   * Returns the instance number.
   *
   * @return
   *   The instance number.
   */
  public int number() {
    return number;
  }

  /**
   * Returns the instance address.
   *
   * @return
   *   The unique instance address.
   */
  public String address() {
    return String.format("%s-%d", componentContext().address(), number());
  }

  /**
   * Returns the parent component context.
   *
   * @return
   *   The parent component context.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T extends ComponentContext> T componentContext() {
    return (T) component;
  }

  @Override
  public String toString() {
    return address();
  }

}
