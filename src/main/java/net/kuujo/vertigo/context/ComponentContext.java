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

import static net.kuujo.vertigo.util.Component.deserializeType;
import static net.kuujo.vertigo.util.Component.isModuleName;
import static net.kuujo.vertigo.util.Component.isVerticleMain;
import static net.kuujo.vertigo.util.Component.serializeType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;

import net.kuujo.vertigo.component.Component;
import net.kuujo.vertigo.hooks.ComponentHook;
import net.kuujo.vertigo.serializer.Serializable;
import net.kuujo.vertigo.serializer.Serializer;
import net.kuujo.vertigo.serializer.SerializerFactory;

/**
 * A component context which contains information regarding each
 * component instance within a single network component. Contexts
 * are immutable as they are constructed once a network has been
 * deployed.
 *
 * @author Jordan Halterman
 */
@SuppressWarnings("rawtypes")
public class ComponentContext<T extends net.kuujo.vertigo.component.Component> implements Serializable {
  private String id;
  private String address;
  private Class<T> type;
  private String main;
  private Map<String, Object> config;
  private List<InstanceContext<T>> instances = new ArrayList<>();
  private long heartbeat = 5000;
  private List<ComponentHook> hooks = new ArrayList<>();
  private List<InputContext> inputs = new ArrayList<>();
  private @JsonIgnore NetworkContext network;

  private ComponentContext() {
  }

  /**
   * Creates a component context from JSON.
   *
   * @param context
   *   A JSON representation of the component context.
   * @return
   *   A component context instance.
   * @throws MalformedContextException
   *   If the context is malformed.
   */
  @SuppressWarnings("unchecked")
  public static <T extends Component<T>> ComponentContext<T> fromJson(JsonObject context) {
    Serializer<ComponentContext> serializer = SerializerFactory.getSerializer(ComponentContext.class);
    ComponentContext<T> component = serializer.deserialize(context.getObject("component"));
    NetworkContext network = NetworkContext.fromJson(context);
    return component.setNetworkContext(network);
  }

  /**
   * Serializes a component context to JSON.
   *
   * @param context
   *   The component context to serialize.
   * @return
   *   A Json representation of the component context.
   */
  public static JsonObject toJson(ComponentContext context) {
    Serializer<ComponentContext> serializer = SerializerFactory.getSerializer(ComponentContext.class);
    JsonObject json = NetworkContext.toJson(context.getNetwork());
    json.putObject("component", serializer.serialize(context));
    return json;
  }

  /**
   * Sets the component parent.
   */
  ComponentContext setNetworkContext(NetworkContext network) {
    this.network = network;
    return this;
  }

  /**
   * Gets the component identifier.
   *
   * @return
   *   The component id.
   */
  public String id() {
    return id;
  }

  @Deprecated
  public String getAddress() {
    return address();
  }

  /**
   * Gets the component address.
   *
   * @return
   *   The component address.
   */
  public String address() {
    return address;
  }

  @Deprecated
  public Class<T> getType() {
    return type();
  }

  /**
   * Gets the component type.
   *
   * @return
   *   The component type.
   */
  public Class<T> type() {
    return type;
  }

  @JsonGetter("type")
  private String getSerializedType() {
    return serializeType(type);
  }

  @JsonSetter("type")
  @SuppressWarnings("unchecked")
  private void setSerializedType(String type) {
    this.type = (Class<T>) deserializeType(type);
  }

  /**
   * Returns a boolean indicating whether the component is a module.
   *
   * @return
   *   Indicates whether the component is a module.
   */
  public boolean isModule() {
    return main != null && isModuleName(main);
  }

  @Deprecated
  public String getModule() {
    return module();
  }

  /**
   * Gets the component module name.
   *
   * @return
   *   The component module name.
   */
  public String module() {
    return isModule() ? main : null;
  }

  /**
   * Helper method for language modules for which "module" is a keyword.
   *
   * @see #module()
   */
  public String moduleName() {
    return module();
  }

  /**
   * Returns a boolean indicating whether the component is a verticle.
   *
   * @return
   *   Indicates whether the component is a verticle.
   */
  public boolean isVerticle() {
    return main != null && isVerticleMain(main);
  }

  @Deprecated
  public String getMain() {
    return main();
  }

  /**
   * Gets the component verticle main.
   *
   * @return
   *   The component verticle main.
   */
  public String main() {
    return isVerticle() ? main : null;
  }

  @Deprecated
  public JsonObject getConfig() {
    return config();
  }

  /**
   * Gets the component configuration.
   *
   * @return
   *   The component configuration.
   */
  public JsonObject config() {
    return config != null ? new JsonObject(config).copy() : new JsonObject();
  }

  @Deprecated
  public int getNumInstances() {
    return numInstances();
  }

  /**
   * Gets the number of component instances.
   *
   * @return
   *   The number of component instances.
   */
  public int numInstances() {
    return instances.size();
  }

  @Deprecated
  public List<InstanceContext<T>> getInstances() {
    return instanceContexts();
  }

  /**
   * Gets a list of all component instances.
   *
   * @return
   *   A list of component instance contexts.
   */
  public List<InstanceContext<T>> instanceContexts() {
    for (InstanceContext<T> instance : instances) {
      instance.setComponentContext(this);
    }
    return instances;
  }

  @Deprecated
  public InstanceContext<T> getInstance(String id) {
    return instanceContext(id);
  }

  /**
   * Gets a component instance by ID.
   *
   * @param id
   *   The instance ID.
   * @return
   *   A component instance or null if the instance doesn't exist.
   */
  public InstanceContext<T> instanceContext(String id) {
    for (InstanceContext<T> instance : instances) {
      if (instance.id().equals(id)) {
        return instance.setComponentContext(this);
      }
    }
    return null;
  }

  @Deprecated
  public long getHeartbeatInterval() {
    return heartbeatInterval();
  }

  /**
   * Gets the component heartbeat interval.
   *
   * @return
   *   The component heartbeat interval.
   */
  public long heartbeatInterval() {
    return heartbeat;
  }

  @Deprecated
  public List<ComponentHook> getHooks() {
    return hooks();
  }

  /**
   * Gets a list of component hooks.
   *
   * @return
   *   A list of component hooks.
   */
  public List<ComponentHook> hooks() {
    return hooks;
  }

  @Deprecated
  public List<InputContext> getInputs() {
    return inputContexts();
  }

  /**
   * Returns a list of component inputs.
   *
   * @return
   *   A list of component inputs.
   */
  public List<InputContext> inputContexts() {
    for (InputContext input : inputs) {
      input.setComponentContext(this);
    }
    return inputs;
  }

  @Deprecated
  public NetworkContext getNetwork() {
    return networkContext();
  }

  /**
   * Returns the parent network context.
   *
   * @return
   *   The parent network context.
   */
  public NetworkContext networkContext() {
    return network;
  }

  @Override
  public String toString() {
    return id();
  }

}
