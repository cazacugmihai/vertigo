/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.vertigo.cluster;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A cluster deployment.
 * 
 * @author Jordan Halterman
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = ModuleDeploymentInfo.class, name = "module"),
    @JsonSubTypes.Type(value = VerticleDeploymentInfo.class, name = "verticle") })
@JsonAutoDetect(creatorVisibility = JsonAutoDetect.Visibility.NONE, fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface DeploymentInfo {

  /**
   * A deployment type.
   * 
   * @author Jordan Halterman
   */
  public static enum Type {

    /**
     * A module deployment type.
     */
    MODULE("module"),

    /**
     * A verticle deployment type.
     */
    VERTICLE("verticle");

    private final String name;

    private Type(String name) {
      this.name = name;
    }

    /**
     * Returns the deployment type name.
     * 
     * @return The deployment type name.
     */
    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }

  }

  /**
   * Returns the deployment ID.
   * 
   * @return The deployment ID.
   */
  String id();

  /**
   * Returns a set of deployment targets.
   * 
   * @return A set of deployment targets.
   */
  Set<String> targets();

  /**
   * Returns the deployment type.
   * 
   * @return The deployment type.
   */
  Type type();

  /**
   * Returns a boolean indicating whether the deployment is a module.
   * 
   * @return Indicates whether the deployment is a module.
   */
  boolean isModule();

  /**
   * Returns a boolean indicating whether the deployment is a verticle.
   * 
   * @return Indicates whether the deployment is a verticle.
   */
  boolean isVerticle();

  /**
   * Returns the deployment configuration.
   * 
   * @return The deployment configuration.
   */
  JsonObject config();

  /**
   * Returns the number of deployment instances.
   * 
   * @return The number of deployment instances.
   */
  int instances();

  /**
   * A deployment info builder.
   * 
   * @author Jordan Halterman
   */
  public static class Builder {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final JsonObject info;

    private Builder() {
      info = new JsonObject();
    }

    private Builder(JsonObject info) {
      this.info = info;
    }

    /**
     * Returns a new deployment info builder.
     * 
     * @return A new builder instance.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Returns a new deployment info builder.
     * 
     * @param info Json info with which to start the build.
     * @return A new builder instance.
     */
    public static Builder newBuilder(JsonObject info) {
      return new Builder(info);
    }

    /**
     * Returns a new deployment info builder.
     * 
     * @param info Existing node info with which to initialize the builder.
     * @return A new builder instance.
     */
    public static Builder newBuilder(DeploymentInfo info) {
      try {
        return new Builder(new JsonObject(mapper.writeValueAsString(info)));
      }
      catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }

    /**
     * Sets the deployment ID.
     * 
     * @param id The deployment ID.
     * @return The builder instance.
     */
    public Builder setId(String id) {
      info.putString("id", id);
      return this;
    }

    /**
     * Sets the deployment type.
     * 
     * @param type The deployment type.
     * @return The builder instance.
     */
    public Builder setType(Type type) {
      info.putString("type", type.getName());
      return this;
    }

    /**
     * Sets the deployment targets.
     * 
     * @param targets The deployment targets.
     * @return The builder instance.
     */
    public Builder setTargets(String... targets) {
      info.putArray("targets", new JsonArray(targets));
      return this;
    }

    /**
     * Sets the deployment targets.
     * 
     * @param targets The deployment targets.
     * @return The builder instance.
     */
    public Builder setTargets(Set<String> targets) {
      info.putArray("targets", new JsonArray(targets.toArray(new String[targets.size()])));
      return this;
    }

    /**
     * Adds a deployment target.
     * 
     * @param target The deployment target.
     * @return The builder instance.
     */
    public Builder addTarget(String target) {
      if (!info.containsField("targets")) {
        info.putArray("targets", new JsonArray());
      }
      JsonArray targets = info.getArray("targets");
      if (!targets.contains(target)) {
        targets.add(target);
      }
      return this;
    }

    /**
     * Adds deployment targets.
     * 
     * @param targets The deployment targets.
     * @return The builder instance.
     */
    public Builder addTargets(String... targets) {
      for (String target : targets) {
        addTarget(target);
      }
      return this;
    }

    /**
     * Adds deployment targets.
     * 
     * @param targets The deployment targets.
     * @return The builder instance.
     */
    public Builder addTargets(Set<String> targets) {
      for (String target : targets) {
        addTarget(target);
      }
      return this;
    }

    /**
     * Removes a deployment target.
     * 
     * @param target The deployment target.
     * @return The builder instance.
     */
    public Builder removeTarget(String target) {
      if (!info.containsField("targets")) {
        info.putArray("targets", new JsonArray());
      }
      JsonArray targets = info.getArray("targets");
      Iterator<Object> iterator = targets.iterator();
      while (iterator.hasNext()) {
        if (iterator.next().equals(target)) {
          iterator.remove();
        }
      }
      return this;
    }

    /**
     * Removes deployment targets.
     * 
     * @param targets The deployment targets.
     * @return The builder instance.
     */
    public Builder removeTargets(String... targets) {
      for (String target : targets) {
        removeTarget(target);
      }
      return this;
    }

    /**
     * Removes deployment targets.
     * 
     * @param targets The deployment targets.
     * @return The builder instance.
     */
    public Builder removeTargets(Set<String> targets) {
      for (String target : targets) {
        removeTarget(target);
      }
      return this;
    }

    /**
     * Sets the deployment configuration.
     * 
     * @param config The deployment configuration.
     * @return The builder instance.
     */
    public Builder setConfig(JsonObject config) {
      info.putObject("config", config);
      return this;
    }

    /**
     * Sets the number of deployment instances.
     * 
     * @param instances The number of deployment instances.
     * @return The builder instance.
     */
    public Builder setInstances(int instances) {
      info.putNumber("instances", instances);
      return this;
    }

    /**
     * Sets the deployment module.
     * 
     * @param moduleName The deployment module.
     * @return The builder instance.
     */
    public Builder setModule(String moduleName) {
      info.putString("module", moduleName);
      return this;
    }

    /**
     * Sets the deployment verticle main.
     * 
     * @param main The deployment verticle main.
     * @return The builder instance.
     */
    public Builder setMain(String main) {
      info.putString("main", main);
      return this;
    }

    /**
     * Sets whether the deployment is a worker verticle.
     * 
     * @param isWorker Indicates whether the deployment is a worker.
     * @return The builder instance.
     */
    public Builder setWorker(boolean isWorker) {
      info.putBoolean("worker", isWorker);
      return this;
    }

    /**
     * Sets whether the deployment is a multi-threaded worker verticle.
     * 
     * @param isMultiThreaded Indicates whether the deployment is multi-threaded.
     * @return The builder instance.
     */
    public Builder setMultiThreaded(boolean isMultiThreaded) {
      setWorker(true);
      info.putBoolean("multi-threaded", isMultiThreaded);
      return this;
    }

    /**
     * Builds the deployment info.
     * 
     * @return A new deployment info instance.
     */
    public DeploymentInfo build() {
      try {
        return mapper.readValue(info.encode(), DeploymentInfo.class);
      }
      catch (IOException e) {
        throw new ClusterException(e);
      }
    }
  }

}
