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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

import net.kuujo.vertigo.logging.Level;
import net.kuujo.vertigo.serializer.Serializable;

/**
 * Network configuration options.<p>
 *
 * These configuration options apply to both the global network and in some cases
 * each of its components. Default component configuration options are inherited
 * as the default values of network components. Thus, if no specific value is set
 * on a component configuration, it may inherit the configuration from the network
 * configuration.
 *
 * @author Jordan Halterman
 */
public class Config implements Serializable {

  /**
   * The global log level for the network.
   */
  public static final String NETWORK_LOG_LEVEL = "log";

  /**
   * The number of network auditors to use.
   */
  public static final String NETWORK_NUM_AUDITORS = "auditors";

  /**
   * Whether acking is enabled for the network.
   */
  @Deprecated
  public static final String NETWORK_ACKING = "acking";

  /**
   * The maximum number of milliseconds auditors will hold message
   * codes in memory before automatically failing them.
   */
  public static final String NETWORK_ACK_TIMEOUT = "timeout";

  /**
   * A Json container for default component configuration options.
   */
  public static final String COMPONENTS = "components";

  /**
   * The default configuration for all components in the network.
   */
  public static final String COMPONENT_CONFIG = "config";

  /**
   * The default number of instances for all components in the network.
   */
  public static final String COMPONENT_NUM_INSTANCES = "instances";

  private static final long DEFAULT_ACK_TIMEOUT = 30000;

  private Level log = Level.INFO;
  private int auditors = 1;
  private boolean acking = true;
  private long timeout = DEFAULT_ACK_TIMEOUT;
  private ComponentDefaults components = new ComponentDefaults();

  /**
   * Component configuration options.
   *
   * @author Jordan Halterman
   */
  private static class ComponentDefaults implements Serializable {
    private Map<String, Object> config;
    private int instances = 1;
  }

  /**
   * Sets the network log level.
   *
   * @param level
   *   The network log level.
   * @return
   *   The network configuration.
   */
  public Config setLogLevel(Level level) {
    log = level;
    return this;
  }

  /**
   * Sets the network log level.
   *
   * @param level
   *   The network log level.
   * @return
   *   The network configuration.
   */
  @JsonSetter("log")
  public Config setLogLevel(String level) {
    if (level != null) {
      log = Level.parse(level.toUpperCase());
    }
    return this;
  }

  /**
   * Sets the network log level.
   *
   * @param level
   *   The network log level.
   * @return
   *   The network configuration.
   */
  public Config setLogLevel(int level) {
    log = Level.parse(level);
    return this;
  }

  /**
   * Gets the network log level.
   *
   * @return
   *   The current network log level. Defaults to INFO.
   */
  public Level getLogLevel() {
    return log;
  }

  @JsonGetter("log")
  private String getLogLevelString() {
    return log.getName();
  }

  /**
   * Enables acking on the network.
   *
   * When acking is enabled, network auditors will track message trees throughout
   * the network and notify messages sources once messages have completed processing.
   *
   * @return
   *   The called network configuration.
   */
  @Deprecated
  public Config enableAcking() {
    return setAckingEnabled(true);
  }

  /**
   * Disables acking on the network.
   *
   * When acking is disabled, messages will not be tracked through networks. This
   * essentially meands that all messages will be assumed to have been successfully
   * processed. Disable acking at your own risk.
   *
   * @return
   *   The called network configuration.
   */
  @Deprecated
  public Config disableAcking() {
    return setAckingEnabled(false);
  }

  /**
   * Sets acking on the network.
   *
   * @param enabled
   *   Whether acking is enabled for the network.
   * @return
   *   The called network configuration.
   */
  @Deprecated
  public Config setAckingEnabled(boolean enabled) {
    acking = enabled;
    return this;
  }

  /**
   * Returns a boolean indicating whether acking is enabled.
   *
   * @return
   *   Indicates whether acking is enabled for the network.
   */
  public boolean isAckingEnabled() {
    return acking;
  }

  /**
   * Returns the number of network auditors.
   *
   * @return
   *   The number of network auditors.
   */
  public int getNumAuditors() {
    return auditors;
  }

  /**
   * Sets the number of network auditors.
   *
   * This is the number of auditor verticle instances that will be used to track
   * messages throughout a network. The Vertigo message tracking algorithm is
   * designed to be extremely memory efficient, so it's unlikely that memory will
   * be an issue. However, if performance of your network is an issue (particularly
   * in larger networks) you may need to increase the number of network auditors.
   *
   * @param numAuditors
   *   The number of network auditors.
   * @return
   *   The called network configuration.
   */
  public Config setNumAuditors(int numAuditors) {
    this.auditors = numAuditors;
    return this;
  }

  /**
   * Gets the network ack timeout.
   *
   * @return
   *   Ack timeout for the network. Defaults to 30000
   */
  public long getAckTimeout() {
    return timeout;
  }

  /**
   * Sets the network ack timeout.
   *
   * This indicates the maximum amount of time an auditor will hold message
   * information in memory before considering it to be timed out.
   *
   * @param timeout
   *   An ack timeout.
   * @return
   *   The called network configuration.
   */
  public Config setAckTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  /**
   * Gets the default number of network component instances.
   *
   * @return
   *   The default number of component instances.
   */
  public int getDefaultNumInstances() {
    return components.instances;
  }

  /**
   * Sets the default number of network component instances.
   *
   * @param numInstances
   *   The default number of component instances.
   * @return
   *   The network configuration.
   */
  public Config setDefaultNumInstances(int numInstances) {
    components.instances = numInstances;
    return this;
  }

  /**
   * Gets the default network component configuration.
   *
   * @return
   *   The default component configuration.
   */
  public JsonObject getDefaultConfig() {
    return components.config != null ? new JsonObject(components.config) : new JsonObject();
  }

  /**
   * Sets the default network component configuration.
   *
   * @param config
   *   The default component configuration.
   * @return
   *   The network configuration.
   */
  public Config setDefaultConfig(JsonObject config) {
    components.config = config.toMap();
    return this;
  }

}
