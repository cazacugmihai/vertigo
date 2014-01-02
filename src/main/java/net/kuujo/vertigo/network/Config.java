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
import net.kuujo.vertigo.serializer.SerializationException;
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
   * The network identifier format. Defaults to <code>%1$s</code><p>
   * <code>%1</code> is the network address
   */
  public static final String NETWORK_ID_FORMAT = "id";

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
   * <code>%1</code> is the network address<p>
   * <code>%2</code> is the network identifier<p>
   * <code>%3</code> is the component address<p>
   * <code>%4</code> is the component type<p>
   * <code>%5</code> is the component module name or main
   */
  public static final String COMPONENT_ID_FORMAT = "id";

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
   * The instance identifier format. Defaults to <code>%1$s-%3$s-%7$d</code><p>
   * <code>%1</code> is the network address<p>
   * <code>%2</code> is the network identifier<p>
   * <code>%3</code> is the component address<p>
   * <code>%4</code> is the component type<p>
   * <code>%5</code> is the component module name or main<p>
   * <code>%6</code> is the component identifier<p>
   * <code>%7</code> is the instance number
   */
  public static final String INSTANCE_ID_FORMAT = "id";

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
   * @throws MalformedNetworkException
   *   If the configuration is malformed.
   */
  public static Config fromJson(JsonObject json) {
    try {
      return SerializerFactory.getSerializer(Network.class).deserialize(json, Config.class);
    }
    catch (SerializationException e) {
      throw new MalformedNetworkException(e);
    }
  }

  /**
   * Global network configuration container.
   *
   * @author Jordan Halterman
   */
  private static class NetworkConfig implements Serializable {
    private String id = "%1$s";
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
   * Gets the network id string format.
   *
   * @return
   *   The network id format.
   */
  public String getNetworkIdFormat() {
    return network.getIdFormat();
  }

  /**
   * Sets the network id string format.
   *
   * @param format
   *   The network id format.
   * @return
   *   The network configuration.
   */
  public Config setNetworkIdFormat(String format) {
    network.setIdFormat(format);
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
   * Gets the component id string format.
   *
   * @return
   *   The component id format.
   */
  public String getComponentIdFormat() {
    return components.getIdFormat();
  }

  /**
   * Sets the component id string format.
   *
   * @param format
   *   The component id format.
   * @return
   *   The network configuration.
   */
  public Config setComponentIdFormat(String format) {
    components.setIdFormat(format);
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
    private String id = "%1$s-%3$s-%7$d";

    public String getIdFormat() {
      return id;
    }

    public InstanceConfig setIdFormat(String format) {
      id = format;
      return this;
    }

  }

  /**
   * Gets the instance id string format.
   *
   * @return
   *   The instance id format.
   */
  public String getInstanceIdFormat() {
    return instances.getIdFormat();
  }

  /**
   * Sets the instance id string format.
   *
   * @param format
   *   The instance id format.
   * @return
   *   The network configuration.
   */
  public Config setInstanceIdFormat(String format) {
    instances.setIdFormat(format);
    return this;
  }

}
