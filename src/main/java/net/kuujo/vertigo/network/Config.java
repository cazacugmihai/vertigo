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

import java.util.Map;

import org.vertx.java.core.json.JsonObject;

import net.kuujo.vertigo.serializer.Serializable;
import net.kuujo.vertigo.serializer.Serializer;
import net.kuujo.vertigo.serializer.SerializerFactory;

/**
 * Network configuration options.
 *
 * @author Jordan Halterman
 */
public class Config implements Serializable {

  /**
   * A Json container for network configuration options.
   */
  public static final String NETWORK = "network";

  /**
   * The network identifier format. Defaults to <code>%1$s</code>.<p>
   * <code>%1</code> is the network name.
   */
  public static final String NETWORK_ID_FORMAT = "id";

  /**
   * The network address format. Defaults to <code>%1$s</code><p>
   * <code>%1</code> is the network name.
   */
  public static final String NETWORK_ADDRESS_FORMAT = "address";

  /**
   * Whether acking is enabled for the network.
   */
  @Deprecated
  public static final String NETWORK_ACKING_ENABLED = "acking";

  /**
   * The number of network auditors to use.
   */
  public static final String NETWORK_NUM_AUDITORS = "auditors";

  /**
   * The maximum number of milliseconds auditors will hold message
   * codes in memory before automatically failing them.
   */
  public static final String NETWORK_ACK_TIMEOUT = "timeout";

  /**
   * A Json container for component configuration options.
   */
  public static final String COMPONENTS = "components";

  /**
   * The component identifier format. Defaults to <code>%1$s-%3$s</code><p>
   * <code>%1</code> is the network name<p>
   * <code>%2</code> is the component type<p>
   * <code>%3</code> is the component name<p>
   * <code>%4</code> is the component module name or main
   */
  public static final String COMPONENT_ID_FORMAT = "id";

  /**
   * The component address format. Defaults to <code>%1$s.%3$s</code><p>
   * <code>%1</code> is the network name<p>
   * <code>%2</code> is the component type<p>
   * <code>%3</code> is the component name<p>
   * <code>%4</code> is the component module name or main
   */
  public static final String COMPONENT_ADDRESS_FORMAT = "address";

  /**
   * The default configuration for all components in the network.
   */
  public static final String COMPONENT_DEFAULT_CONFIG = "config";

  /**
   * The default number of instances for all components in the network.
   */
  public static final String COMPONENT_DEFAULT_NUM_INSTANCES = "instances";

  /**
   * The default interval at which components will send heartbeat messages to network monitors.
   */
  public static final String COMPONENT_DEFAULT_HEARTBEAT_INTERVAL = "heartbeat";

  /**
   * A Json container for instance configuration options.
   */
  public static final String INSTANCES = "instances";

  /**
   * The instance identifier format. Defaults to <code>%1$s-%2$s-%3$d</code><p>
   * <code>%1</code> is the network name<p>
   * <code>%2</code> is the component type<p>
   * <code>%3</code> is the component name<p>
   * <code>%4</code> is the component module name or main<p>
   * <code>%5</code> is the instance number
   */
  public static final String INSTANCE_ID_FORMAT = "id";

  /**
   * The instance address format. Defaults to <code>%1$s.%2$s.%3$d</code><p>
   * <code>%1</code> is the network name<p>
   * <code>%2</code> is the component type<p>
   * <code>%3</code> is the component name<p>
   * <code>%4</code> is the component module name or main<p>
   * <code>%5</code> is the instance number
   */
  public static final String INSTANCE_ADDRESS_FORMAT  = "address";

  private NetworkConfig network = new NetworkConfig();
  private ComponentConfig components = new ComponentConfig();
  private InstanceConfig instances = new InstanceConfig();

  /**
   * Creates a configuration object from Json.
   *
   * @param json
   *   A Json representation of the configuration.
   * @return
   *   A configuration instance constructed from Json.
   */
  public static Config fromJson(JsonObject json) {
    Serializer<Config> serializer = SerializerFactory.getSerializer(Config.class);
    return serializer.deserialize(json);
  }

  /**
   * Global network configuration container.
   *
   * @author Jordan Halterman
   */
  private static class NetworkConfig implements Serializable {
    private String id = "%1$s";
    private String address = "%1$s";
    private boolean acking = true;
    private int auditors = 1;
    private long timeout = 30000;

    public String getIdFormat() {
      return id;
    }

    public NetworkConfig setIdFormat(String format) {
      id = format;
      return this;
    }

    public String getAddressFormat() {
      return address;
    }

    public NetworkConfig setAddressFormat(String format) {
      address = format;
      return this;
    }

    public boolean isAckingEnabled() {
      return acking;
    }

    public NetworkConfig setAckingEnabled(boolean ackingEnabled) {
      acking = ackingEnabled;
      return this;
    }

    public int getNumAuditors() {
      return auditors;
    }

    public NetworkConfig setNumAuditors(int numAuditors) {
      auditors = numAuditors;
      return this;
    }

    public long getAckTimeout() {
      return timeout;
    }

    public NetworkConfig setAckTimeout(long ackTimeout) {
      timeout = ackTimeout;
      return this;
    }

  }

  /**
   * Gets the network identifier string format.
   *
   * @return
   *   The network identifier format.
   */
  public String getNetworkIdFormat() {
    return network.getIdFormat();
  }

  /**
   * Sets the network identifier string format.
   *
   * @param format
   *   The network identifier format.
   * @return
   *   The network configuration.
   */
  public Config setNetworkIdFormat(String format) {
    network.setIdFormat(format);
    return this;
  }

  /**
   * Gets the network address string format.
   *
   * @return
   *   The network address format.
   */
  public String getNetworkAddressFormat() {
    return network.getAddressFormat();
  }

  /**
   * Sets the network address string format.
   *
   * @param format
   *   The network address format.
   * @return
   *   The network configuration.
   */
  public Config setNetworkAddressFormat(String format) {
    network.setAddressFormat(format);
    return this;
  }

  /**
   * Indicates whether acking is enabled on the network.
   *
   * @return
   *   Whether acking is enabled on the network.
   */
  @Deprecated
  public boolean isNetworkAckingEnabled() {
    return network.isAckingEnabled();
  }

  /**
   * Sets acking on the network.
   *
   * @param ackingEnabled
   *   Indicates whether acking should be enabled or disabled.
   * @return
   *   The network configuration.
   */
  @Deprecated
  public Config setNetworkAckingEnabled(boolean ackingEnabled) {
    network.setAckingEnabled(ackingEnabled);
    return this;
  }

  /**
   * Gets the number of network auditors.
   *
   * @return
   *   The number of network auditors.
   */
  public int getNetworkNumAuditors() {
    return network.getNumAuditors();
  }

  /**
   * Sets the number of network auditors.
   *
   * @param numAuditors
   *   The number of network auditors. This number should be increased to improve
   *   the performance of tracking messages through a network if necessary.
   * @return
   *   The network configuration.
   */
  public Config setNetworkNumAuditors(int numAuditors) {
    network.setNumAuditors(numAuditors);
    return this;
  }

  /**
   * Gets the network ack timeout.
   *
   * @return
   *   The network ack timeout. This is the maximum amount of time an auditor will
   *   wait for a message and all of its descendants to be "acked" before timing
   *   out the message.
   */
  public long getNetworkAckTimeout() {
    return network.getAckTimeout();
  }

  /**
   * Sets the network ack timeout.
   *
   * @param ackTimeout
   *   The network ack timeout. This is the maximum amount of time an auditor will
   *   wait for a message and all of its descendants to be "acked" before timing
   *   out the message.
   * @return
   *   The network configuration.
   */
  public Config setNetworkAckTimeout(long ackTimeout) {
    network.setAckTimeout(ackTimeout);
    return this;
  }

  /**
   * Global component configuration container.
   *
   * @author Jordan Halterman
   */
  private static class ComponentConfig implements Serializable {
    private String id = "%1$s-%3$s";
    private String address = "%1$s.%3$s";
    private Map<String, Object> config;
    private int instances = 1;
    private long heartbeat = 5000;

    public String getIdFormat() {
      return id;
    }

    public ComponentConfig setIdFormat(String format) {
      id = format;
      return this;
    }

    public String getAddressFormat() {
      return address;
    }

    public ComponentConfig setAddressFormat(String format) {
      address = format;
      return this;
    }

    public JsonObject getDefaultConfig() {
      return config != null ? new JsonObject(config) : new JsonObject();
    }

    public ComponentConfig setDefaultConfig(JsonObject config) {
      if (config != null) {
        this.config = config.toMap();
      }
      else {
        this.config = null;
      }
      return this;
    }

    public int getDefaultNumInstances() {
      return instances;
    }

    public ComponentConfig setDefaultNumInstance(int numInstances) {
      instances = numInstances;
      return this;
    }

    public long getDefaultHeartbeatInterval() {
      return heartbeat;
    }

    public ComponentConfig setDefaultHeartbeatInterval(long interval) {
      heartbeat = interval;
      return this;
    }

  }

  /**
   * Gets the component identifier string format.
   *
   * @return
   *   The component identifier format.
   */
  public String getComponentIdFormat() {
    return components.getIdFormat();
  }

  /**
   * Sets the component identifier string format.
   *
   * @param format
   *   The component identifier format.
   * @return
   *   The network configuration.
   */
  public Config setComponentIdFormat(String format) {
    components.setIdFormat(format);
    return this;
  }

  /**
   * Gets the component address string format.
   *
   * @return
   *   The component address format.
   */
  public String getComponentAddressFormat() {
    return components.getAddressFormat();
  }

  /**
   * Sets the component address string format.
   *
   * @param format
   *   The component address format.
   * @return
   *   The network configuration.
   */
  public Config setComponentAddressFormat(String format) {
    components.setAddressFormat(format);
    return this;
  }

  /**
   * Gets the default component configuration.
   *
   * @return
   *   The default component configuration. If no default has been set then an
   *   empty {@link JsonObject} instance will be returned.
   */
  public JsonObject getComponentDefaultConfig() {
    return components.getDefaultConfig();
  }

  /**
   * Sets the default component configuration.
   *
   * @param config
   *   The default component configuration. If this value is <code>null</code> then
   *   the default configuration will be an empty {@link JsonObject} instance.
   * @return
   *   The network configuration.
   */
  public Config setComponentDefaultConfig(JsonObject config) {
    components.setDefaultConfig(config);
    return this;
  }

  /**
   * Gets the default number of component instance.
   *
   * @return
   *   The default number of component instances.
   */
  public int getComponentDefaultNumInstances() {
    return components.getDefaultNumInstances();
  }

  /**
   * Sets the default number of component instances.
   *
   * @param numInstances
   *   The default number of component instances. This number will be applied to
   *   all network components that do not have a number of instances that is
   *   explicitly set.
   * @return
   *   The network configuration.
   */
  public Config setComponentDefaultNumInstances(int numInstances) {
    components.setDefaultNumInstance(numInstances);
    return this;
  }

  /**
   * Gets the default heartbeat interval for all network components.
   *
   * @return
   *   The default heartbeat interval for all network components. Defaults to <code>5000</code>
   *   milliseconds.
   */
  public long getComponentDefaultHeartbeatInterval() {
    return components.getDefaultHeartbeatInterval();
  }

  /**
   * Sets the default heartbeat interval for all network components.
   *
   * @param interval
   *   The default heartbeat interval for all network components. This heartbeat
   *   interval will be applied to all components that do not have a heartbeat
   *   interval that has been explicitly set.
   * @return
   *   The network configuration.
   */
  public Config setComponentDefaultHeartbeatInterval(long interval) {
    components.setDefaultHeartbeatInterval(interval);
    return this;
  }

  /**
   * Global instance configuration container.
   *
   * @author Jordan Halterman
   */
  private static class InstanceConfig implements Serializable {
    private String id = "%1$s-%3$s-%5$d";
    private String address = "%1$s.%3$s.%5$d";

    public String getIdFormat() {
      return id;
    }

    public InstanceConfig setIdFormat(String format) {
      id = format;
      return this;
    }

    public String getAddressFormat() {
      return address;
    }

    public InstanceConfig setAddressFormat(String format) {
      address = format;
      return this;
    }

  }

  /**
   * Gets the instance identifier string format.
   *
   * @return
   *   The instance identifier format.
   */
  public String getInstanceIdFormat() {
    return instances.getIdFormat();
  }

  /**
   * Sets the instance identifier string format.
   *
   * @param format
   *   The instance identifier format.
   * @return
   *   The network configuration.
   */
  public Config setInstanceIdFormat(String format) {
    instances.setIdFormat(format);
    return this;
  }

  /**
   * Gets the instance address string format.
   *
   * @return
   *   The instance address format.
   */
  public String getInstanceAddressFormat() {
    return instances.getAddressFormat();
  }

  /**
   * Sets the instance address string format.
   *
   * @param format
   *   The instance address format.
   * @return
   *   The network configuration.
   */
  public Config setInstanceAddressFormat(String format) {
    instances.setAddressFormat(format);
    return this;
  }

}
