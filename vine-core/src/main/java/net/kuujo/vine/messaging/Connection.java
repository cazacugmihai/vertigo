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
package net.kuujo.vine.messaging;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;

/**
 * A single point-to-point connection.
 *
 * @author Jordan Halterman
 */
public interface Connection {

  /**
   * Gets the remote connection address.
   *
   * @return
   *   The remote connection address.
   */
  public String getAddress();

  /**
   * Sends a message through the connection.
   *
   * @param message
   *   The message to send.
   */
  public Connection send(JsonMessage message);

  /**
   * Sends a message through the connection, providing a handler for a return value.
   *
   * @param message
   *   The message to send.
   * @param replyHandler
   *   A message reply handler.
   */
  public <T> Connection send(JsonMessage message, Handler<AsyncResult<Message<T>>> replyHandler);

  /**
   * Sends a message through the connection, providing a handler for a return value.
   *
   * @param message
   *   The message to send.
   * @param timeout
   *   A message timeout.
   * @param replyHandler
   *   A message reply handler.
   */
  public <T> Connection send(JsonMessage message, long timeout, Handler<AsyncResult<Message<T>>> replyHandler);

  /**
   * Sends a message through the connection, providing a handler for a return value.
   *
   * @param message
   *   The message to send.
   * @param timeout
   *   A message timeout.
   * @param retry
   *   Indicates whether to retry sending the message if sending times out.
   * @param replyHandler
   *   A message reply handler.
   */
  public <T> Connection send(JsonMessage message, long timeout, boolean retry, Handler<AsyncResult<Message<T>>> replyHandler);

  /**
   * Sends a message through the connection, providing a handler for a return value.
   *
   * @param message
   *   The message to send.
   * @param timeout
   *   A message timeout.
   * @param retry
   *   Indicates whether to retry sending the message if sending times out.
   * @param attempts
   *   Indicates the number of times to retry if retries are enabled.
   * @param replyHandler
   *   A message reply handler.
   */
  public <T> Connection send(JsonMessage message, long timeout, boolean retry, int attempts, Handler<AsyncResult<Message<T>>> replyHandler);

}