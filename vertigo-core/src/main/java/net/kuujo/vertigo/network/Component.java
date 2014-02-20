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
package net.kuujo.vertigo.network;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import net.kuujo.vertigo.hooks.ComponentHook;
import net.kuujo.vertigo.input.grouping.Grouping;
import net.kuujo.vertigo.serializer.Serializable;

/**
 * A network component definition.
 * <p>
 * 
 * Components are the primary elements of processing in Vertigo. They can be represented
 * as <code>feeders</code>, <code>executors</code>, or <code>workers</code>. Each network
 * may consist of any number of components, each of which may subscribe to the output of
 * other components inside or outside of the network. Internally, components are
 * represented as Vert.x modules or verticles. Just as with Vert.x modules and verticles,
 * components may consist of several instances.
 * <p>
 * 
 * @author Jordan Haltermam
 */
@JsonTypeInfo(
  use=JsonTypeInfo.Id.NAME,
  include=JsonTypeInfo.As.PROPERTY,
  property="deploy"
)
@JsonSubTypes({
  @JsonSubTypes.Type(value=Module.class, name=Component.COMPONENT_DEPLOYMENT_MODULE),
  @JsonSubTypes.Type(value=Verticle.class, name=Component.COMPONENT_DEPLOYMENT_VERTICLE)
})
public abstract class Component<T extends Component<T>> implements Config {

  /**
   * <code>address</code> is a string indicating the globally unique component event bus
   * address. Components will use this address to register a handler which listens for
   * subscriptions from other components, so this address must be unique across a Vert.x
   * cluster.
   */
  public static final String COMPONENT_ADDRESS = "address";

  /**
   * <code>type</code> is a string indicating the type of component that will be deployed.
   * This can be either <code>feeder</code>, <code>worker</code>, or <code>executor</code>
   * . If the component type does not match the implementation then an error will occur
   * upon deployment of the component. This field is required.
   */
  public static final String COMPONENT_TYPE = "type";

  /**
   * <code>deploy</code> is a string indicating the deployment method for the component.
   * This can be either <code>module</code> or <code>verticle</code>. This field is
   * required.
   */
  public static final String COMPONENT_DEPLOYMENT_METHOD = "deploy";

  /**
   * <code>module</code> is the module deployment method.
   */
  public static final String COMPONENT_DEPLOYMENT_MODULE = "module";

  /**
   * <code>verticle</code> is the verticle deployment method.
   */
  public static final String COMPONENT_DEPLOYMENT_VERTICLE = "verticle";

  /**
   * <code>config</code> is an object defining the configuration to pass to each instance
   * of the component. If no configuration is provided then an empty configuration will be
   * passed to component instances.
   */
  public static final String COMPONENT_CONFIG = "config";

  /**
   * <code>instances</code> is a number indicating the number of instances of the
   * component to deploy. Defaults to <code>1</code>
   */
  public static final String COMPONENT_NUM_INSTANCES = "instances";

  /**
   * <code>targets</code> is an array of target nodes to which this component can be
   * deployed. If this array is empty or null, the component can be deployed to any node
   * within a cluster.
   */
  public static final String COMPONENT_TARGETS = "targets";

  /**
   * <code>heartbeat</code> is a number indicating the interval at which the component
   * should send heartbeat messages to network monitors (in milliseconds). Defaults to
   * <code>5000</code> milliseconds.
   */
  public static final String COMPONENT_HEARTBEAT_INTERVAL = "heartbeat";

  /**
   * <code>hooks</code> is an array of hook configurations. Each hook configuration must
   * contain at least a <code>type</code> key which indicates the fully qualified name of
   * the hook class. Other configuration options depend on the specific hook
   * implementation. In most cases, json properties are directly correlated to fields
   * within the hook class.
   */
  public static final String COMPONENT_HOOKS = "hooks";

  /**
   * <code>inputs</code> is an array of input configurations. Each input configuration
   * must contain the <code>address</code> to which the input subscribes. Additionally,
   * each configuration may contain a <code>grouping</code> field indicating the input
   * grouping method. This must be an object containing at least a <code>type</code>
   * field. The <code>type</code> field can be one of <code>round</code>,
   * <code>random</code>, <code>fields</code>, or <code>all</code>. The
   * <code>grouping</code> defaults to <code>round</code>. Finally, a <code>stream</code>
   * field may be provided to indicate the stream to which to subscribe. This field
   * defaults to <code>default</code>.
   */
  public static final String COMPONENT_INPUTS = "inputs";

  /**
   * Component type.
   * 
   * @author Jordan Halterman
   */
  public static enum Type {

    /**
     * A feeder component.
     */
    FEEDER("feeder"),

    /**
     * A worker component.
     */
    WORKER("worker");

    private final String name;

    private Type(String name) {
      this.name = name;
    }

    /**
     * Returns the component type name.
     * 
     * @return The component type name.
     */
    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }

    /**
     * Parses a component type name.
     * 
     * @param name The component type name.
     * @return A component type.
     * @throws IllegalArgumentException If the compoennt type name is invalid.
     */
    public static Type parse(String name) {
      switch (name) {
        case "feeder":
          return FEEDER;
        case "worker":
          return WORKER;
        default:
          throw new IllegalArgumentException("Invalid component type " + name);
      }
    }

  }

  private static final int DEFAULT_NUM_INSTANCES = 1;
  private static final long DEFAULT_HEARTBEAT_INTERVAL = 5000;

  private String address;
  private Type type;
  private Map<String, Object> config;
  private int instances = DEFAULT_NUM_INSTANCES;
  private Set<String> targets = new HashSet<>();
  private long heartbeat = DEFAULT_HEARTBEAT_INTERVAL;
  private List<ComponentHook> hooks = new ArrayList<>();
  private List<Input> inputs = new ArrayList<>();

  public Component() {
    address = UUID.randomUUID().toString();
  }

  public Component(Type type, String address) {
    this.type = type;
    this.address = address;
  }

  @SuppressWarnings("unchecked")
  T setAddress(String address) {
    this.address = address;
    return (T) this;
  }

  /**
   * Returns the component deployment type.
   */
  @JsonGetter("deploy")
  protected abstract String getDeploymentType();

  /**
   * Returns the component address.
   * 
   * This address is an event bus address at which the component will register a handler
   * to listen for connections when started. Thus, this address must be unique.
   * 
   * @return The component address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Gets the component type.
   * <p>
   * 
   * The component type is a type that indicates the nature of the component
   * implementation (module or verticle). For instance, if the component is a
   * <code>Feeder</code> component, the module or verticle must be a
   * <code>FeederVerticle</code> instance.
   * 
   * @return The component type.
   */
  public Type getType() {
    return type;
  }

  @JsonGetter("type")
  private String getSerializedType() {
    return type.getName();
  }

  @JsonSetter("type")
  private void setSerializedType(String type) {
    this.type = Type.parse(type);
  }

  /**
   * Returns a boolean indicating whether the component is a module.
   * 
   * @return Indicates whether the component is a module.
   */
  public boolean isModule() {
    return false;
  }

  /**
   * Returns a boolean indicating whether the component is a verticle.
   * 
   * @return Indicates whether the component is a verticle.
   */
  public boolean isVerticle() {
    return false;
  }

  /**
   * Returns the component configuration.
   * 
   * @return The component configuration.
   */
  public JsonObject getConfig() {
    return config != null ? new JsonObject(config) : new JsonObject();
  }

  /**
   * Sets the component configuration.
   * <p>
   * 
   * This configuration will be passed to component implementations as the verticle or
   * module configuration when the component is started.
   * 
   * @param config The component configuration.
   * @return The component configuration.
   */
  @SuppressWarnings("unchecked")
  public T setConfig(JsonObject config) {
    this.config = config.toMap();
    return (T) this;
  }

  /**
   * Returns the number of component instances to deploy within the network.
   * 
   * @return The number of component instances.
   */
  public int getNumInstances() {
    return instances;
  }

  /**
   * Sets the number of component instances to deploy within the network.
   * 
   * @param numInstances The number of component instances.
   * @return The component configuration.
   */
  @SuppressWarnings("unchecked")
  public T setNumInstances(int numInstances) {
    instances = numInstances;
    for (Input input : inputs) {
      input.setCount(instances);
    }
    return (T) this;
  }

  /**
   * Adds a deployment target to the component.
   * 
   * @param node The node to add to the component.
   * @return The component configuration.
   */
  @SuppressWarnings("unchecked")
  public T addDeploymentTarget(String node) {
    targets.add(node);
    return (T) this;
  }

  /**
   * Adds a set of deployment targets to the component.
   * 
   * @param nodes A list of nodes to which the component may deploy.
   * @return The component configuration.
   */
  @SuppressWarnings("unchecked")
  public T setDeploymentTargets(String... nodes) {
    this.targets = new HashSet<>();
    for (String node : nodes) {
      targets.add(node);
    }
    return (T) this;
  }

  /**
   * Returns a set of deployment targets for the component.
   * 
   * @return A set of event bus addresses indicating nodes to which the component can be
   *         deployed.
   */
  public Set<String> getDeploymentTargets() {
    return targets;
  }

  /**
   * Returns the component heartbeat interval. This is the interval at which component
   * instances will send heartbeat messages to network monitors.
   * 
   * @return The component heartbeat interval.
   */
  public long getHeartbeatInterval() {
    return heartbeat;
  }

  /**
   * Sets the component heartbeat interval. This is the interval at which component
   * instances will send heartbeat messages to network monitors.
   * 
   * @param interval The component heartbeat interval.
   * @return The component configuration.
   */
  @SuppressWarnings("unchecked")
  public T setHeartbeatInterval(long interval) {
    heartbeat = interval;
    return (T) this;
  }

  /**
   * Adds a component hook to the component.
   * 
   * The output hook can be used to receive notifications on events that occur within the
   * component instance's inputs and outputs. Hooks should implement the
   * {@link ComponentHook} interface. Hook state will be automatically serialized to json
   * using an internal <code>Serializer</code>. By default, this means that any
   * primitives, primitive wrappers, collections, or {@link Serializable} fields will be
   * serialized. Finer grained control over serialization of hooks can be provided by
   * either using Jackson annotations within the hook implementation or by providing a
   * custom serializer for the hook.
   * 
   * @param hook A component hook.
   * @return The component configuration.
   * @see ComponentHook
   */
  @SuppressWarnings("unchecked")
  public T addHook(ComponentHook hook) {
    hooks.add(hook);
    return (T) this;
  }

  /**
   * Returns a list of all component hooks.
   * 
   * @return A list of component hooks.
   */
  public List<ComponentHook> getHooks() {
    return hooks;
  }

  /**
   * Gets a list of component inputs.
   * 
   * @return A list of component inputs.
   */
  public List<Input> getInputs() {
    return inputs;
  }

  /**
   * Adds a component input.
   * 
   * @param input The input to add.
   * @return The new input instance.
   */
  public Input addInput(Input input) {
    inputs.add(input);
    return input;
  }

  /**
   * Adds a component input from another component.
   * 
   * @param component The component from which to receive input.
   * @return The new input instance.
   */
  public Input addInput(Component<?> component) {
    return addInput(new Input(component.getAddress()));
  }

  /**
   * Adds a component input from another component on a specific stream.
   * 
   * @param component The component from which to receive input.
   * @param stream The stream on which to receive input.
   * @return The new input instance.
   */
  public Input addInput(Component<?> component, String stream) {
    return addInput(new Input(component.getAddress(), stream));
  }

  /**
   * Adds a component input from another component.
   * 
   * @param component The component from which to receive input.
   * @param grouping The grouping by which to group the input.
   * @return The new input instance.
   */
  public Input addInput(Component<?> component, Grouping grouping) {
    return addInput(new Input(component.getAddress()).groupBy(grouping));
  }

  /**
   * Adds a component input from another component on a specific stream.
   * 
   * @param component The component from which to receive input.
   * @param stream The stream on which to receive input.
   * @param grouping The grouping by which to group the input.
   * @return The new input instance.
   */
  public Input addInput(Component<?> component, String stream, Grouping grouping) {
    return addInput(new Input(component.getAddress(), stream).groupBy(grouping));
  }

  /**
   * Adds a component input on the default stream.
   * 
   * @param address The input address. This is the event bus address of a component to
   *          which this component will listen for output.
   * @return The new input instance.
   */
  public Input addInput(String address) {
    return addInput(new Input(address));
  }

  /**
   * Adds a component input on a specific stream.
   * 
   * @param address The input address. This is the event bus address of a component to
   *          which this component will listen for output.
   * @param stream The stream on which to receive input.
   * @return The new input instance.
   */
  public Input addInput(String address, String stream) {
    return addInput(new Input(address, stream));
  }

  /**
   * Adds a component input on the default stream with a grouping.
   * 
   * @param address The input address. This is the event bus address of a component to
   *          which this component will listen for output.
   * @param grouping An input grouping. This input grouping helps determine how messages
   *          will be distributed among multiple instances of this component.
   * @return The new input instance.
   */
  public Input addInput(String address, Grouping grouping) {
    return addInput(new Input(address).groupBy(grouping));
  }

  /**
   * Adds a component input on a specific stream with a grouping.
   * 
   * @param address The input address. This is the event bus address of a component to
   *          which this component will listen for output.
   * @param stream The stream on which to receive input.
   * @param grouping An input grouping. This input grouping helps determine how messages
   *          will be distributed among multiple instances of this component.
   * @return The new input instance.
   */
  public Input addInput(String address, String stream, Grouping grouping) {
    return addInput(new Input(address, stream).groupBy(grouping));
  }

  @Override
  public String toString() {
    return getAddress();
  }

}
