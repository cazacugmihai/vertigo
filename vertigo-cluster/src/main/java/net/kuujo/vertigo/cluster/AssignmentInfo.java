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

import net.kuujo.vertigo.cluster.impl.DefaultAssignmentInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Assignment info.
 * 
 * @author Jordan Halterman
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class", defaultImpl = DefaultAssignmentInfo.class)
@JsonAutoDetect(creatorVisibility = JsonAutoDetect.Visibility.NONE, fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface AssignmentInfo {

  /**
   * Returns the assignment instance.
   * 
   * @return The assignment instance.
   */
  InstanceInfo instance();

  /**
   * An assignment info builder.
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
     * Returns a new assignment info builder.
     * 
     * @return A new builder instance.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Returns a new assignment info builder.
     * 
     * @param info Json info with which to start the build.
     * @return A new builder instance.
     */
    public static Builder newBuilder(JsonObject info) {
      return new Builder(info);
    }

    /**
     * Returns a new assignment info builder.
     * 
     * @param info Existing assignment info with which to initialize the builder.
     * @return A new builder instance.
     */
    public static Builder newBuilder(AssignmentInfo info) {
      try {
        return new Builder(new JsonObject(mapper.writeValueAsString(info)));
      }
      catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }

    /**
     * Sets the assignment instance.
     * 
     * @param info The assignment instance info.
     * @return The builder instance.
     */
    public Builder setInstance(InstanceInfo info) {
      try {
        this.info.putObject("instance", new JsonObject(mapper.writeValueAsString(info)));
      }
      catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
      return this;
    }

    /**
     * Builds the assignment info.
     * 
     * @return A new assignment info instance.
     */
    public AssignmentInfo build() {
      try {
        return mapper.readValue(info.encode(), AssignmentInfo.class);
      }
      catch (IOException e) {
        throw new ClusterException(e);
      }
    }
  }

}
