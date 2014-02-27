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

import org.vertx.java.core.json.JsonObject;

import net.kuujo.vertigo.cluster.impl.DefaultInstanceInfo;
import net.kuujo.vertigo.util.serializer.Serializable;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deployment instance info.
 * 
 * @author Jordan Halterman
 */
@JsonTypeInfo(
  use=JsonTypeInfo.Id.CLASS,
  include=JsonTypeInfo.As.PROPERTY,
  property="class",
  defaultImpl=DefaultInstanceInfo.class
)
public interface InstanceInfo extends Serializable {

  /**
   * Returns the instance ID.
   * 
   * @return The unique instance ID.
   */
  String id();

  /**
   * Returns the parent deployment info.
   * 
   * @return The parent deployment info.
   */
  DeploymentInfo deployment();

  /**
   * An instance info builder.
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
     * Returns a new instance info builder.
     * 
     * @return A new builder instance.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Returns a new instance info builder.
     * 
     * @param info Json info with which to start the build.
     * @return A new builder instance.
     */
    public static Builder newBuilder(JsonObject info) {
      return new Builder(info);
    }

    /**
     * Returns a new instance info builder.
     * 
     * @param info Existing instance info with which to initialize the builder.
     * @return A new builder instance.
     */
    public static Builder newBuilder(InstanceInfo info) {
      try {
        return new Builder(new JsonObject(mapper.writeValueAsString(info)));
      }
      catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }

    /**
     * Sets the instance ID.
     * 
     * @param id The instance ID.
     * @return The builder instance.
     */
    public Builder setId(String id) {
      info.putString("id", id);
      return this;
    }

    /**
     * Sets the instance's parent deployment.
     * 
     * @param info The parent deployment info.
     * @return The builder instance.
     */
    public Builder setDeployment(DeploymentInfo info) {
      try {
        this.info.putObject("deployment", new JsonObject(mapper.writeValueAsString(info)));
      }
      catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
      return this;
    }

    /**
     * Builds the instance info.
     * 
     * @return An instance info instance.
     */
    public InstanceInfo build() {
      try {
        return mapper.readValue(info.encode(), InstanceInfo.class);
      }
      catch (IOException e) {
        throw new ClusterException(e);
      }
    }
  }

}
