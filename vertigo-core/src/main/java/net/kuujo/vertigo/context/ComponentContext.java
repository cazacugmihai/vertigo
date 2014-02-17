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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import net.kuujo.vertigo.hooks.ComponentHook;
import net.kuujo.vertigo.network.Component;
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
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="deploy")
@JsonSubTypes({
  @JsonSubTypes.Type(value=ModuleContext.class, name=Component.COMPONENT_DEPLOYMENT_MODULE),
  @JsonSubTypes.Type(value=VerticleContext.class, name=Component.COMPONENT_DEPLOYMENT_VERTICLE)
})
public abstract class ComponentContext<T extends ComponentContext<T>> implements Context {
  private String address;
  private Component.Type type;
  private Map<String, Object> config;
  private List<InstanceContext> instances = new ArrayList<>();
  private long heartbeat = 5000;
  private Set<String> targets = new HashSet<>();
  private List<ComponentHook> hooks = new ArrayList<>();
  private List<InputContext> inputs = new ArrayList<>();
  private @JsonIgnore NetworkContext network;

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
  public static <T extends ComponentContext<T>> T fromJson(JsonObject context) {
    Serializer serializer = SerializerFactory.getSerializer(ComponentContext.class);
    T component = (T) serializer.deserialize(context.getObject("component"), ComponentContext.class);
    NetworkContext network = NetworkContext.fromJson(context);
    return (T) component.setNetworkContext(network);
  }

  /**
   * Serializes a component context to JSON.
   *
   * @param context
   *   The component context to serialize.
   * @return
   *   A Json representation of the component context.
   */
  public static <T extends ComponentContext<T>> JsonObject toJson(ComponentContext<T> context) {
    Serializer serializer = SerializerFactory.getSerializer(ComponentContext.class);
    JsonObject json = NetworkContext.toJson(context.networkContext());
    json.putObject("component", serializer.serialize(context));
    return json;
  }

  /**
   * Returns the component deployment type.
   */
  @JsonGetter("deploy")
  protected abstract String getDeploymentType();

  /**
   * Sets the component parent.
   */
  @SuppressWarnings("unchecked")
  T setNetworkContext(NetworkContext network) {
    this.network = network;
    return (T) this;
  }

  /**
   * Gets the unique component address.
   *
   * @return
   *   The component address.
   */
  public String address() {
    return address;
  }

  /**
   * Gets the component type.
   *
   * @return
   *   The component type.
   */
  public Component.Type type() {
    return type;
  }

  @JsonGetter("type")
  private String getSerializedType() {
    return type.getName();
  }

  @JsonSetter("type")
  private void setSerializedType(String type) {
    this.type = Component.Type.parse(type);
  }

  /**
   * Returns a boolean indicating whether the component is a module.
   *
   * @return
   *   Indicates whether the component is a module.
   */
  public boolean isModule() {
    return false;
  }

  /**
   * Returns a boolean indicating whether the component is a verticle.
   *
   * @return
   *   Indicates whether the component is a verticle.
   */
  public boolean isVerticle() {
    return false;
  }

  /**
   * Gets the component configuration.
   *
   * @return
   *   The component configuration.
   */
  public JsonObject config() {
    return config != null ? new JsonObject(config) : new JsonObject();
  }

  /**
   * Gets a list of all component instance contexts.
   *
   * @return
   *   A list of component instance contexts.
   */
  public List<InstanceContext> instanceContexts() {
    for (InstanceContext instance : instances) {
      instance.setComponentContext(this);
    }
    return instances;
  }

  /**
   * Returns the number of component instances.
   *
   * @return
   *   The number of component instances.
   */
  public int numInstances() {
    return instances.size();
  }

  /**
   * Gets a component instance context by instance ID.
   *
   * @param id
   *   The instance ID.
   * @return
   *   A component instance or <code>null</code> if the instance doesn't exist.
   */
  public InstanceContext instanceContext(int instanceNumber) {
    for (InstanceContext instance : instances) {
      if (instance.number() == instanceNumber) {
        return instance.setComponentContext(this);
      }
    }
    return null;
  }

  /**
   * Gets a component instance context by instance address.
   *
   * @param address
   *   The instance address.
   * @return
   *   A component instance or <code>null</code> if the instance doesn't exist.
   */
  public InstanceContext instanceContext(String address) {
    for (InstanceContext instance : instances) {
      if (instance.address().equals(address)) {
        return instance.setComponentContext(this);
      }
    }
    return null;
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

  /**
   * Returns a set of deployment targets for the component.
   *
   * @return
   *   A set of event bus addresses indicating nodes to which the
   *   component can be deployed.
   */
  public Set<String> getDeploymenTargets() {
    return targets;
  }

  /**
   * Returns a boolean indicating whether the component can be
   * deployed to a specific node.
   *
   * @param address
   *   The address of the node to check.
   * @return
   *   Indicates whether instances of the component can be deployed
   *   to the given node.
   */
  public boolean hasDeploymentTarget(String address) {
    return targets.contains(address);
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

  /**
   * Returns a list of component input contexts.
   *
   * @return
   *   A list of component input contexts.
   */
  public List<InputContext> inputContexts() {
    for (InputContext input : inputs) {
      input.setComponentContext(this);
    }
    return inputs;
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
    return address();
  }

}
