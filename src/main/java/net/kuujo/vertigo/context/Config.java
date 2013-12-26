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

import java.util.Map;

import net.kuujo.vertigo.logging.Level;
import net.kuujo.vertigo.serializer.Serializable;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * An immutable network configuration.
 *
 * @author Jordan Halterman
 */
public class Config implements Serializable {
  private static final long DEFAULT_ACK_TIMEOUT = 30000;
  private Level log;
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
   * Gets the network log level.
   *
   * @return
   *   The network log level.
   */
  public Level logLevel() {
    return log;
  }

  @JsonGetter("log")
  private String getLogLevelString() {
    return log != null ? log.getName() : null;
  }

  @JsonSetter("log")
  private void setLogLevelString(String level) {
    if (level != null) {
      log = Level.parse(level);
    }
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
  public int numAuditors() {
    return auditors;
  }

  /**
   * Gets the network ack timeout.
   *
   * @return
   *   Ack timeout for the network. Defaults to 30000
   */
  public long ackTimeout() {
    return timeout;
  }

  /**
   * Gets the default number of network component instances.
   *
   * @return
   *   The default number of component instances.
   */
  public int defaultNumInstances() {
    return components.instances;
  }

  /**
   * Gets the default network component configuration.
   *
   * @return
   *   The default component configuration.
   */
  public JsonObject defaultConfig() {
    return components.config != null ? new JsonObject(components.config) : new JsonObject();
  }

}
