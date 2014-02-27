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

import net.kuujo.vertigo.util.serializer.Serializable;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A base request.
 * 
 * @author Jordan Halterman
 */
public abstract class Request implements Serializable {
  @JsonIgnore
  protected Message<?> message;

  /**
   * Replies to the request with a generic object.
   * 
   * @param result The request result.
   */
  public void reply(JsonObject result) {
    message.reply(new JsonObject().putString("status", "ok").putString("result", result.encode()));
  }

  /**
   * Replies to the request with an error.
   * 
   * @param message The error message.
   */
  public void error(String message) {
    this.message.reply(new JsonObject().putString("status", "error").putString("message", message));
  }

  /**
   * Replies to the request with an error.
   * 
   * @param error The error that occurred.
   */
  public void error(Throwable error) {
    this.error(error.getMessage());
  }

}
