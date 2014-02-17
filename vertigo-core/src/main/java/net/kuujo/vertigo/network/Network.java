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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.kuujo.vertigo.serializer.SerializationException;
import net.kuujo.vertigo.serializer.SerializerFactory;
import static net.kuujo.vertigo.util.Component.isModuleName;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * A Vertigo network configuration.
 * <p>
 * 
 * A network is a collection of <code>components</code> - Vert.x verticles or modules -
 * that are interconnected in a meaningful and reliable manner. This class is used to
 * define such structures.
 * 
 * @author Jordan Halterman
 */
public final class Network implements Config {

  /**
   * <code>address</code> is a string indicating the unique network address. This is the
   * address at which the network will monitor network components. This field is required.
   */
  public static final String NETWORK_ADDRESS = "address";

  /**
   * <code>auditors</code> is a number indicating the number of auditor instances to
   * deploy with the network. Auditors are used to track messages through the network, and
   * increasing the number of auditors may help improve performace in networks with high
   * message volume. Defaults to <code>1</code>
   */
  public static final String NETWORK_NUM_AUDITORS = "auditors";

  /**
   * <code>acking</code> is a boolean indicating whether acking is enabled for the
   * network. If acking is disabled then messages will not be tracked through the network.
   * Instead, messages will be immediately "completed" once they have been emitted from a
   * component. Defaults to <code>true</code> (acking enabled).
   */
  public static final String NETWORK_ACKING_ENABLED = "acking";

  /**
   * <code>timeouts</code> is a boolean indicating whether message timeouts are enabled
   * for the network. If message timeouts are disabled then auditors will never time out
   * messages. Ack and failure mechanisms will continue to work. Defaults to
   * <code>true</code> (timweouts enabled).
   */
  public static final String NETWORK_MESSAGE_TIMEOUTS_ENABLED = "timeouts";

  /**
   * <code>timeout</code> is a number indicating the number of milliseconds after which a
   * not-yet-completed message should be timed out. Defaults to <code>30000</code> (30
   * seconds).
   */
  public static final String NETWORK_MESSAGE_TIMEOUT = "timeout";

  /**
   * <code>components</code> is an object defining network component configurations. Each
   * item in the object must be keyed by the unique component address, with each item
   * being an object containing the component configuration. See the {@link Component}
   * class for component configuration options.
   */
  public static final String NETWORK_COMPONENTS = "components";

  private static final int DEFAULT_NUM_AUDITORS = 1;
  private static final long DEFAULT_MESSAGE_TIMEOUT = 30000;

  private String address;
  private int auditors = DEFAULT_NUM_AUDITORS;
  private boolean acking = true;
  private long timeout = DEFAULT_MESSAGE_TIMEOUT;
  private Map<String, Component<?>> components = new HashMap<String, Component<?>>();

  public Network() {
    address = UUID.randomUUID().toString();
  }

  public Network(String address) {
    this.address = address;
  }

  /**
   * Creates a network from JSON.
   * 
   * @param json A JSON representation of the network.
   * @return A new network configuration.
   * @throws MalformedNetworkException If the network definition is malformed.
   */
  public static Network fromJson(JsonObject json) {
    try {
      return SerializerFactory.getSerializer(Network.class).deserialize(json, Network.class);
    }
    catch (SerializationException e) {
      throw new MalformedNetworkException(e);
    }
  }

  /**
   * Returns the network address.
   * 
   * This is the event bus address at which the network's coordinator will register a
   * handler for components to connect to once deployed.
   * 
   * @return The network address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Enables acking on the network.
   * 
   * When acking is enabled, network auditors will track message trees throughout the
   * network and notify messages sources once messages have completed processing.
   * 
   * @return The network configuration.
   */
  public Network enableAcking() {
    acking = true;
    return this;
  }

  /**
   * Disables acking on the network.
   * 
   * When acking is disabled, messages will not be tracked through networks. This
   * essentially meands that all messages will be assumed to have been successfully
   * processed. Disable acking at your own risk.
   * 
   * @return The network configuration.
   */
  public Network disableAcking() {
    acking = false;
    return this;
  }

  /**
   * Sets acking on the network.
   * 
   * @param enabled Whether acking is enabled for the network.
   * @return The network configuration.
   */
  public Network setAckingEnabled(boolean enabled) {
    acking = enabled;
    return this;
  }

  /**
   * Returns a boolean indicating whether acking is enabled.
   * 
   * @return Indicates whether acking is enabled for the network.
   */
  public boolean isAckingEnabled() {
    return acking;
  }

  /**
   * Returns the number of network auditors.
   * 
   * @return The number of network auditors.
   */
  public int getNumAuditors() {
    return auditors;
  }

  /**
   * Sets the number of network auditors.
   * 
   * This is the number of auditor verticle instances that will be used to track messages
   * throughout a network. The Vertigo message tracking algorithm is designed to be
   * extremely memory efficient, so it's unlikely that memory will be an issue. However,
   * if performance of your network is an issue (particularly in larger networks) you may
   * need to increase the number of network auditors.
   * 
   * @param numAuditors The number of network auditors.
   * @return The network configuration.
   */
  public Network setNumAuditors(int numAuditors) {
    this.auditors = numAuditors;
    return this;
  }

  /**
   * Enables message timeouts for the network.
   * 
   * @return The network configuration.
   */
  public Network enableMessageTimeouts() {
    if (timeout == 0) {
      timeout = DEFAULT_MESSAGE_TIMEOUT;
    }
    return this;
  }

  /**
   * Disables message timeouts for the network.
   * 
   * @return The network configuration.
   */
  public Network disableMessageTimeouts() {
    timeout = 0;
    return this;
  }

  /**
   * Sets whether message timeouts are enabled for the network.
   * 
   * @param isEnabled Indicates whether to enable message timeouts.
   * @return The network configuration.
   */
  @JsonSetter("timeouts")
  public Network setMessageTimeoutsEnabled(boolean isEnabled) {
    if (isEnabled) {
      return enableMessageTimeouts();
    }
    else {
      return disableMessageTimeouts();
    }
  }

  /**
   * Returns a boolean indicating whether message timeouts are enabled for the network.
   * 
   * @return Indicates whether message timeouts are enabled.
   */
  public boolean isMessageTimeoutsEnabled() {
    return timeout > 0;
  }

  /**
   * Sets the network message timeout.
   * 
   * This indicates the maximum amount of time an auditor will hold message information in
   * memory before considering it to be timed out.
   * 
   * @param timeout A message timeout in milliseconds.
   * @return The network configuration.
   */
  public Network setMessageTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  /**
   * Gets the network message timeout.
   * 
   * @return The message timeout for the network in milliseconds. Defaults to 30000
   */
  public long getMessageTimeout() {
    return timeout;
  }

  /**
   * Gets a list of network components.
   * 
   * @return A list of network components.
   */
  public List<Component<?>> getComponents() {
    List<Component<?>> components = new ArrayList<Component<?>>();
    for (Map.Entry<String, Component<?>> entry : this.components.entrySet()) {
      components.add(entry.getValue().setAddress(entry.getKey()));
    }
    return components;
  }

  /**
   * Gets a component by address.
   * 
   * @param address The component address.
   * @return The component configuration.
   * @throws IllegalArgumentException If the given component address does not exist within
   *           the network.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T extends Component> T getComponent(String address) {
    if (components.containsKey(address)) {
      return (T) components.get(address).setAddress(address);
    }
    throw new IllegalArgumentException(address + " is not a valid component address in " + getAddress());
  }

  /**
   * Adds a component to the network.
   * 
   * @param component The component to add.
   * @return The added component configuration.
   */
  public <T extends Component<T>> T addComponent(T component) {
    components.put(component.getAddress(), component);
    return component;
  }

  /**
   * Adds a module to the network.
   * 
   * @param module The module to add.
   * @return The added module component configuration.
   */
  public Module addModule(Module module) {
    components.put(module.getAddress(), module);
    return module;
  }

  /**
   * Adds a verticle to the network.
   * 
   * @param verticle The verticle to add.
   * @return The added verticle component configuration.
   */
  public Verticle addVerticle(Verticle verticle) {
    components.put(verticle.getAddress(), verticle);
    return verticle;
  }

  /**
   * Adds a feeder component to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleOrMain The feeder component main or module name. Vertigo will
   *          automatically detect whether the feeder is a module or a verticle based on
   *          module naming conventions.
   * @return The new feeder component configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends Component<T>> T addFeeder(String address, String moduleOrMain) {
    if (isModuleName(moduleOrMain)) {
      return (T) addFeederModule(address, moduleOrMain);
    }
    else {
      return (T) addFeederVerticle(address, moduleOrMain);
    }
  }

  /**
   * Adds a feeder component to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleOrMain The feeder component main or module name. Vertigo will
   *          automatically detect whether the feeder is a module or a verticle based on
   *          module naming conventions.
   * @param config The feeder component configuration. This configuration will be made
   *          available as the verticle configuration within the component implementation.
   * @return The new feeder component configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends Component<T>> T addFeeder(String address, String moduleOrMain, JsonObject config) {
    if (isModuleName(moduleOrMain)) {
      return (T) addFeederModule(address, moduleOrMain, config);
    }
    else {
      return (T) addFeederVerticle(address, moduleOrMain, config);
    }
  }

  /**
   * Adds a feeder component to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleOrMain The feeder component main or module name. Vertigo will
   *          automatically detect whether the feeder is a module or a verticle based on
   *          module naming conventions.
   * @param numInstances The number of feeder instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new feeder component configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends Component<T>> T addFeeder(String address, String moduleOrMain, int numInstances) {
    if (isModuleName(moduleOrMain)) {
      return (T) addFeederModule(address, moduleOrMain, numInstances);
    }
    else {
      return (T) addFeederVerticle(address, moduleOrMain, numInstances);
    }
  }

  /**
   * Adds a feeder component to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleOrMain The feeder component main or module name. Vertigo will
   *          automatically detect whether the feeder is a module or a verticle based on
   *          module naming conventions.
   * @param config The feeder component configuration. This configuration will be made
   *          available as the verticle configuration within the component implementation.
   * @param numInstances The number of feeder instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new feeder component configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends Component<T>> T addFeeder(String address, String moduleOrMain, JsonObject config, int numInstances) {
    if (isModuleName(moduleOrMain)) {
      return (T) addFeederModule(address, moduleOrMain, config, numInstances);
    }
    else {
      return (T) addFeederVerticle(address, moduleOrMain, config, numInstances);
    }
  }

  /**
   * Adds a feeder module to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleName The feeder module name.
   * @return The new feeder module configuration.
   * @throws IllegalArgumentException If the module name is not a valid module identifier.
   */
  public Module addFeederModule(String address, String moduleName) {
    return addModule(new Module(Component.Type.FEEDER, address, moduleName));
  }

  /**
   * Adds a feeder module to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleName The feeder module name.
   * @param config The feeder module configuration. This configuration will be made
   *          available as the verticle configuration within deployed module instances.
   * @return The new feeder module configuration.
   * @throws IllegalArgumentException If the module name is not a valid module identifier.
   */
  public Module addFeederModule(String address, String moduleName, JsonObject config) {
    return addModule(new Module(Component.Type.FEEDER, address, moduleName).setConfig(config));
  }

  /**
   * Adds a feeder module to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleName The feeder module name.
   * @param numInstances The number of module instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new feeder module configuration.
   * @throws IllegalArgumentException If the module name is not a valid module identifier.
   */
  public Module addFeederModule(String address, String moduleName, int numInstances) {
    return addModule(new Module(Component.Type.FEEDER, address, moduleName).setNumInstances(numInstances));
  }

  /**
   * Adds a feeder module to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleName The feeder module name.
   * @param config The feeder module configuration. This configuration will be made
   *          available as the verticle configuration within deployed module instances.
   * @param numInstances The number of module instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new feeder module configuration.
   * @throws IllegalArgumentException If the module name is not a valid module identifier.
   */
  public Module addFeederModule(String address, String moduleName, JsonObject config, int numInstances) {
    return addModule(new Module(Component.Type.FEEDER, address, moduleName).setConfig(config).setNumInstances(numInstances));
  }

  /**
   * Adds a feeder verticle to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param main The feeder verticle main.
   * @return The new feeder verticle configuration.
   */
  public Verticle addFeederVerticle(String address, String main) {
    return addVerticle(new Verticle(Component.Type.FEEDER, address, main));
  }

  /**
   * Adds a feeder verticle to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param main The feeder verticle main.
   * @param config The feeder verticle configuration. This configuration will be made
   *          available as the verticle configuration within deployed module instances.
   * @return The new feeder verticle configuration.
   */
  public Verticle addFeederVerticle(String address, String main, JsonObject config) {
    return addVerticle(new Verticle(Component.Type.FEEDER, address, main).setConfig(config));
  }

  /**
   * Adds a feeder verticle to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param main The feeder verticle main.
   * @param numInstances The number of verticle instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new feeder verticle configuration.
   */
  public Verticle addFeederVerticle(String address, String main, int numInstances) {
    return addVerticle(new Verticle(Component.Type.FEEDER, address, main).setNumInstances(numInstances));
  }

  /**
   * Adds a feeder verticle to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param main The feeder verticle main.
   * @param config The feeder verticle configuration. This configuration will be made
   *          available as the verticle configuration within deployed module instances.
   * @param numInstances The number of verticle instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new feeder verticle configuration.
   */
  public Verticle addFeederVerticle(String address, String main, JsonObject config, int numInstances) {
    return addVerticle(new Verticle(Component.Type.FEEDER, address, main).setConfig(config).setNumInstances(numInstances));
  }

  /**
   * Adds a worker component to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleOrMain The worker component main or module name. Vertigo will
   *          automatically detect whether the worker is a module or a verticle based on
   *          module naming conventions.
   * @return The new worker component configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends Component<T>> T addWorker(String address, String moduleOrMain) {
    if (isModuleName(moduleOrMain)) {
      return (T) addWorkerModule(address, moduleOrMain);
    }
    else {
      return (T) addWorkerVerticle(address, moduleOrMain);
    }
  }

  /**
   * Adds a worker component to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleOrMain The worker component main or module name. Vertigo will
   *          automatically detect whether the worker is a module or a verticle based on
   *          module naming conventions.
   * @param config The worker component configuration. This configuration will be made
   *          available as the verticle configuration within the component implementation.
   * @return The new worker component configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends Component<T>> T addWorker(String address, String moduleOrMain, JsonObject config) {
    if (isModuleName(moduleOrMain)) {
      return (T) addWorkerModule(address, moduleOrMain, config);
    }
    else {
      return (T) addWorkerVerticle(address, moduleOrMain, config);
    }
  }

  /**
   * Adds a worker component to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleOrMain The worker component main or module name. Vertigo will
   *          automatically detect whether the worker is a module or a verticle based on
   *          module naming conventions.
   * @param numInstances The number of worker instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new worker component configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends Component<T>> T addWorker(String address, String moduleOrMain, int numInstances) {
    if (isModuleName(moduleOrMain)) {
      return (T) addWorkerModule(address, moduleOrMain, numInstances);
    }
    else {
      return (T) addWorkerVerticle(address, moduleOrMain, numInstances);
    }
  }

  /**
   * Adds a worker component to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleOrMain The worker component main or module name. Vertigo will
   *          automatically detect whether the worker is a module or a verticle based on
   *          module naming conventions.
   * @param config The worker component configuration. This configuration will be made
   *          available as the verticle configuration within the component implementation.
   * @param numInstances The number of worker instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new worker component configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends Component<T>> T addWorker(String address, String moduleOrMain, JsonObject config, int numInstances) {
    if (isModuleName(moduleOrMain)) {
      return (T) addWorkerModule(address, moduleOrMain, config, numInstances);
    }
    else {
      return (T) addWorkerVerticle(address, moduleOrMain, config, numInstances);
    }
  }

  /**
   * Adds a worker module to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleName The worker module name.
   * @return The new worker module configuration.
   * @throws IllegalArgumentException If the module name is not a valid module identifier.
   */
  public Module addWorkerModule(String address, String moduleName) {
    return addModule(new Module(Component.Type.WORKER, address, moduleName));
  }

  /**
   * Adds a worker module to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleName The worker module name.
   * @param config The worker module configuration. This configuration will be made
   *          available as the verticle configuration within deployed module instances.
   * @return The new worker module configuration.
   * @throws IllegalArgumentException If the module name is not a valid module identifier.
   */
  public Module addWorkerModule(String address, String moduleName, JsonObject config) {
    return addModule(new Module(Component.Type.WORKER, address, moduleName).setConfig(config));
  }

  /**
   * Adds a worker module to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleName The worker module name.
   * @param numInstances The number of module instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new worker module configuration.
   * @throws IllegalArgumentException If the module name is not a valid module identifier.
   */
  public Module addWorkerModule(String address, String moduleName, int numInstances) {
    return addModule(new Module(Component.Type.WORKER, address, moduleName).setNumInstances(numInstances));
  }

  /**
   * Adds a worker module to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param moduleName The worker module name.
   * @param config The worker module configuration. This configuration will be made
   *          available as the verticle configuration within deployed module instances.
   * @param numInstances The number of module instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new worker module configuration.
   * @throws IllegalArgumentException If the module name is not a valid module identifier.
   */
  public Module addWorkerModule(String address, String moduleName, JsonObject config, int numInstances) {
    return addModule(new Module(Component.Type.WORKER, address, moduleName).setConfig(config).setNumInstances(numInstances));
  }

  /**
   * Adds a worker verticle to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param main The worker verticle main.
   * @return The new worker verticle configuration.
   */
  public Verticle addWorkerVerticle(String address, String main) {
    return addVerticle(new Verticle(Component.Type.WORKER, address, main));
  }

  /**
   * Adds a worker verticle to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param main The worker verticle main.
   * @param config The worker verticle configuration. This configuration will be made
   *          available as the verticle configuration within deployed module instances.
   * @return The new worker verticle configuration.
   */
  public Verticle addWorkerVerticle(String address, String main, JsonObject config) {
    return addVerticle(new Verticle(Component.Type.WORKER, address, main).setConfig(config));
  }

  /**
   * Adds a worker verticle to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param main The worker verticle main.
   * @param numInstances The number of verticle instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new worker verticle configuration.
   */
  public Verticle addWorkerVerticle(String address, String main, int numInstances) {
    return addVerticle(new Verticle(Component.Type.WORKER, address, main).setNumInstances(numInstances));
  }

  /**
   * Adds a worker verticle to the network.
   * 
   * @param address The component address. This should be a globally unique event bus
   *          address and can be any string.
   * @param main The worker verticle main.
   * @param config The worker verticle configuration. This configuration will be made
   *          available as the verticle configuration within deployed module instances.
   * @param numInstances The number of verticle instances. If multiple instances are
   *          defined, groupings will be used to determine how messages are distributed
   *          between multiple component instances.
   * @return The new worker verticle configuration.
   */
  public Verticle addWorkerVerticle(String address, String main, JsonObject config, int numInstances) {
    return addVerticle(new Verticle(Component.Type.WORKER, address, main).setConfig(config).setNumInstances(numInstances));
  }

  @Override
  public String toString() {
    return getAddress();
  }

}
