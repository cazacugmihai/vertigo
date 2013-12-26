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
package net.kuujo.vertigo.logging.impl;

import net.kuujo.vertigo.logging.Logger;


/**
 * A Vertigo logger factory.
 *
 * @author Jordan Halterman
 */
public final class LoggerFactory {

  /**
   * Gets a class logger.
   *
   * @param clazz
   *   The class for which to return a logger.
   * @return
   *   A new logger instance.
   */
  public static Logger getLogger(Class<?> clazz) {
    return new Logger(org.vertx.java.core.logging.impl.LoggerFactory.getLogger(clazz));
  }

  /**
   * Gets a named logger.
   *
   * @param name
   *   The logger name.
   * @return
   *   A new logger instance.
   */
  public static Logger getLogger(String name) {
    return new Logger(org.vertx.java.core.logging.impl.LoggerFactory.getLogger(name));
  }

}
