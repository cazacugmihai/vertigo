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
package net.kuujo.vertigo.cluster.protocol;

import net.kuujo.vertigo.util.serializer.Serializer;
import net.kuujo.vertigo.util.serializer.SerializerFactory;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * A ping request.
 * 
 * @author Jordan Halterman
 */
public class PingRequest extends Request {
  private static final Serializer serializer = SerializerFactory.getSerializer(PingRequest.class);
  private long term;
  private String leader;

  public PingRequest() {
  }

  public PingRequest(long term, String leader) {
    this.term = term;
    this.leader = leader;
  }

  public static PingRequest fromJson(String json) {
    return serializer.deserializeFromString(json, PingRequest.class);
  }

  public static PingRequest fromJson(String json, Message<?> message) {
    return serializer.deserializeFromString(json, PingRequest.class).setMessage(message);
  }

  public static String toJson(PingRequest request) {
    return serializer.serializeToString(request);
  }

  private PingRequest setMessage(Message<?> message) {
    this.message = message;
    return this;
  }

  /**
   * Returns the requesting node's current term.
   * 
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the requesting node's current leader.
   *
   * @return The requesting node's current known leader.
   */
  public String leader() {
    return leader;
  }

  /**
   * Replies to the request.
   *
   * @param term The responding node's current term.
   */
  public void reply(long term) {
    reply(new JsonObject().putNumber("term", term));
  }

}
