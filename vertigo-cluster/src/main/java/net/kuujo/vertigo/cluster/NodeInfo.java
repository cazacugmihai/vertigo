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
import java.util.Collection;
import java.util.Iterator;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.vertigo.cluster.impl.DefaultNodeInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Node info.
 * 
 * @author Jordan Halterman
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class", defaultImpl = DefaultNodeInfo.class)
@JsonAutoDetect(creatorVisibility = JsonAutoDetect.Visibility.NONE, fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface NodeInfo {

  /**
   * Returns the node address.
   * 
   * @return The node address.
   */
  String address();

  /**
   * Returns a collection of deployments assigned to the node.
   * 
   * @return A collection of deployments assigned to the node.
   */
  Collection<AssignmentInfo> assignments();

  /**
   * Node info builder.
   * 
   * @author Jordan Halterman
   */
  public static class Builder {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final JsonObject info;

    private Builder() {
      info = new JsonObject().putArray("assignments", new JsonArray());
    }

    private Builder(JsonObject info) {
      this.info = info;
    }

    /**
     * Returns a new node info builder.
     * 
     * @return A new builder instance.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Returns a new node info builder.
     * 
     * @param info Json info with which to start the build.
     * @return A new builder instance.
     */
    public static Builder newBuilder(JsonObject info) {
      return new Builder(info);
    }

    /**
     * Returns a new node info builder.
     * 
     * @param info Existing node info with which to initialize the builder.
     * @return A new builder instance.
     */
    public static Builder newBuilder(NodeInfo info) {
      try {
        return new Builder(new JsonObject(mapper.writeValueAsString(info)));
      }
      catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }

    /**
     * Sets the node address.
     * 
     * @param address The node address.
     * @return The builder instance.
     */
    public Builder setAddress(String address) {
      info.putString("address", address);
      return this;
    }

    /**
     * Sets the node's assignments.
     * 
     * @param assignments A collection of assignment info.
     * @return The builder instance.
     */
    public Builder setAssignments(Collection<AssignmentInfo> assignments) {
      for (AssignmentInfo info : assignments) {
        addAssignment(info);
      }
      return this;
    }

    /**
     * Adds an assignment to the node.
     * 
     * @param info The assignment info.
     * @return The builder instance.
     */
    public Builder addAssignment(AssignmentInfo info) {
      if (!this.info.containsField("assignments")) {
        this.info.putArray("assignments", new JsonArray());
      }
      JsonArray assignments = this.info.getArray("assignments");
      try {
        assignments.add(new JsonObject(mapper.writeValueAsString(info)));
      }
      catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
      return this;
    }

    /**
     * Removes an assignment from the node.
     * 
     * @param info The assignment info.
     * @return The builder instance.
     */
    public Builder removeAssignment(AssignmentInfo info) {
      if (!this.info.containsField("assignments")) {
        this.info.putArray("assignments", new JsonArray());
      }
      JsonArray assignments = this.info.getArray("assignments");
      Iterator<Object> iterator = assignments.iterator();
      while (iterator.hasNext()) {
        try {
          AssignmentInfo compare = mapper.readValue(((JsonObject) iterator.next()).encode(), AssignmentInfo.class);
          if (compare.equals(info)) {
            iterator.remove();
          }
        }
        catch (IOException e) {
          throw new ClusterException(e);
        }
      }
      return this;
    }

    /**
     * Builds the node info.
     * 
     * @return A new node info instance.
     */
    public NodeInfo build() {
      try {
        return mapper.readValue(info.encode(), NodeInfo.class);
      }
      catch (IOException e) {
        throw new ClusterException(e);
      }
    }
  }

}
