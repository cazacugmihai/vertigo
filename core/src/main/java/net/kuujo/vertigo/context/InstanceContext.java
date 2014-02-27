/*
 * Copyright 2013-2014 the original author or authors.
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

import net.kuujo.vertigo.serializer.Serializer;
import net.kuujo.vertigo.serializer.SerializerFactory;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A component instance context which contains information regarding a specific component
 * (module or verticle) instance within a network.
 * 
 * @author Jordan Halterman
 */
public final class InstanceContext extends Context<InstanceContext> {
  private String id;
  private int number;
  private InputContext input;
  private OutputContext output;
  @JsonIgnore
  private ComponentContext<?> component;

  private InstanceContext() {
  }

  /**
   * Creates a new instance context from JSON.
   * 
   * @param context A JSON representation of the instance context.
   * @return A new instance context instance.
   * @throws MalformedContextException If the JSON context is malformed.
   */
  public static InstanceContext fromJson(JsonObject context) {
    Serializer serializer = SerializerFactory.getSerializer(InstanceContext.class);
    InstanceContext instance = serializer.deserializeFromObject(context.getObject("instance"), InstanceContext.class);
    ComponentContext<?> component = ComponentContext.fromJson(context);
    return instance.setComponentContext(component);
  }

  /**
   * Serializes an instance context to JSON.
   * 
   * @param context The instance context to serialize.
   * @return A Json representation of the instance context.
   */
  public static JsonObject toJson(InstanceContext context) {
    Serializer serializer = SerializerFactory.getSerializer(InstanceContext.class);
    JsonObject json = ComponentContext.toJson(context.component().isModule() ?
        context.<ModuleContext>component() : context.<VerticleContext>component());
    return json.putObject("instance", serializer.serializeToObject(context));
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
   * @return The instance ID.
   */
  public String id() {
    return id;
  }

  /**
   * Returns the instance number.
   * 
   * @return The instance number.
   */
  public int number() {
    return number;
  }

  /**
   * Returns the instance address.
   * 
   * @return The unique instance address.
   */
  public String address() {
    return String.format("%s-%d", component().address(), number());
  }

  /**
   * Returns the instance input context.
   *
   * @return The instance input context.
   */
  public InputContext input() {
    return input.setInstanceContext(this);
  }

  /**
   * Returns the instance output context.
   *
   * @return The instance output context.
   */
  public OutputContext output() {
    return output.setInstanceContext(this);
  }

  /**
   * Returns the parent component context.
   * 
   * @return The parent component context.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T extends ComponentContext> T component() {
    return (T) component;
  }

  @Override
  public void notify(InstanceContext update) {
    super.notify(update);
    input.notify(update.input());
    output.notify(update.output());
  }

  @Override
  public String toString() {
    return address();
  }

  /**
   * Instance context builder.
   *
   * @author Jordan Halterman
   */
  public static class Builder {
    private InstanceContext context;

    private Builder() {
      context = new InstanceContext();
    }

    private Builder(InstanceContext context) {
      this.context = context;
    }

    /**
     * Creates a new context builder.
     *
     * @return A new instance context builder.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Creates a new context builder.
     *
     * @param context A starting instance context.
     * @return A new instance context builder.
     */
    public static Builder newBuilder(InstanceContext context) {
      return new Builder(context);
    }

    /**
     * Sets the unique instance ID.
     *
     * @param id A unique ID.
     * @return The context builder.
     */
    public Builder setId(String id) {
      context.id = id;
      return this;
    }

    /**
     * Sets the instance number.
     *
     * @param number The instance number which should be unique to the component.
     * @return The context builder.
     */
    public Builder setNumber(int number) {
      context.number = number;
      return this;
    }

    /**
     * Sets the instance input context.
     *
     * @param input An input context.
     * @return The context builder.
     */
    public Builder setInput(InputContext input) {
      context.input = input;
      return this;
    }

    /**
     * Sets the instance output context.
     *
     * @param output An output context.
     * @return The context builder.
     */
    public Builder setOutput(OutputContext output) {
      context.output = output;
      return this;
    }

    /**
     * Builds the instance context.
     *
     * @return A new instance context.
     */
    public InstanceContext build() {
      return context;
    }
  }

}
