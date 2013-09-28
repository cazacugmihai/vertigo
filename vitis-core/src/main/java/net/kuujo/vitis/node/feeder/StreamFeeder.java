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
package net.kuujo.vitis.node.feeder;

import org.vertx.java.core.Handler;

/**
 * A stream feeder.
 *
 * @author Jordan Halterman
 *
 * @param <T> The feeder type
 */
public interface StreamFeeder<T extends StreamFeeder<?>> extends BasicFeeder<T> {

  /**
   * Indicates whether the feed queue is full.
   *
   * @return
   *   A boolean indicating whether the feed queue is full.
   */
  public boolean feedQueueFull();

  /**
   * Sets a drain handler on the feeder.
   *
   * @param handler
   *   A drain handler to be invoked once the feeder is ready
   *   to accept additional data.
   * @return
   *   The called feeder instance.
   */
  public T drainHandler(Handler<Void> handler);

}