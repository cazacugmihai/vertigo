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
package net.kuujo.vertigo.output;

import net.kuujo.vertigo.context.OutputPortContext;
import net.kuujo.vertigo.hooks.OutputHook;
import net.kuujo.vertigo.message.JsonMessage;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * An input port.
 *
 * @author Jordan Halterman
 */
public interface OutputPort {

  /**
   * Returns the output port name.
   *
   * @return The output port name.
   */
  String name();

  /**
   * Returns the output port context.
   *
   * @return The output port context.
   */
  OutputPortContext context();

  /**
   * Adds a hook to the port.
   *
   * @param hook An output hook.
   * @return The output port.
   */
  OutputPort addHook(OutputHook hook);

  /**
   * Emits a message to the output port.
   *
   * @param body The body of the message to emit.
   * @return The emitted message ID.
   */
  String emit(JsonObject body);

  /**
   * Emits a child message to the output port.
   * 
   * Emitting data as the child of an existing message creates a new node in the parent
   * message's message tree. When the new message is emitted, the auditor assigned to the
   * parent message will be notified of the change, and the new message will be tracked as
   * a child. This means that the parent message will not be considered fully processed
   * until all of its children have been acked and are considered fully processed (their
   * children are acked... etc). It is strongly recommended that users use this API
   * whenever possible.
   *
   * @param body The body of the message to emit.
   * @param parent The parent of the message being emitted.
   * @return The emitted message ID.
   */
  String emit(JsonObject body, JsonMessage parent);

  /**
   * Emits a message to the output port.
   *
   * @param message The message to emit.
   * @return The emitted message ID.
   */
  String emit(JsonMessage message);

  /**
   * Opens the output port.
   *
   * @return The output port.
   */
  OutputPort open();

  /**
   * Opens the output port.
   *
   * @param doneHandler An asynchronous handler to be called once opened.
   * @return The output port.
   */
  OutputPort open(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Closes the output port.
   */
  void close();

  /**
   * Closes the output port.
   *
   * @param doneHandler An asynchronous handler to be called once closed.
   */
  void close(Handler<AsyncResult<Void>> doneHandler);

}