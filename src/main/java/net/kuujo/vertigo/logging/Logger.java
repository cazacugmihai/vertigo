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
package net.kuujo.vertigo.logging;


/**
 * An internal Vertigo logger.
 *
 * @author Jordan Halterman
 */
public class Logger extends org.vertx.java.core.logging.Logger {
  private org.vertx.java.core.logging.Logger logger;
  private Level level = Level.INFO;

  public Logger(org.vertx.java.core.logging.Logger logger) {
    super(null);
    this.logger = logger;
  }

  /**
   * Sets the logger level.
   *
   * @param level
   *   The logger level.
   * @return
   *   The logger instance.
   */
  public Logger setLevel(Level level) {
    this.level = level;
    return this;
  }

  /**
   * Gets the log level.
   *
   * @return
   *   The current log level.
   */
  public Level getLevel() {
    return level;
  }

  @Override
  public boolean isInfoEnabled() {
    return isLoggable(Level.INFO);
  }

  @Override
  public boolean isDebugEnabled() {
    return isLoggable(Level.DEBUG);
  }

  @Override
  public boolean isTraceEnabled() {
    return isLoggable(Level.TRACE);
  }

  /**
   * Indicates whether the given level is loggable according to the current log level.
   */
  private boolean isLoggable(Level level) {
    return level.intValue() >= this.level.intValue();
  }

  @Override
  public void fatal(final Object message) {
    if (isLoggable(Level.FATAL)) {
      logger.fatal(message);
    }
  }

  @Override
  public void fatal(final Object message, final Throwable t) {
    if (isLoggable(Level.FATAL)) {
      logger.fatal(message, t);
    }
  }

  public void fatal(final String format, final Object... args) {
    if (isLoggable(Level.FATAL)) {
      logger.fatal(String.format(format, args));
    }
  }

  @Override
  public void error(final Object message) {
    if (isLoggable(Level.ERROR)) {
      logger.error(message);
    }
  }

  @Override
  public void error(final Object message, final Throwable t) {
    if (isLoggable(Level.ERROR)) {
      logger.error(message, t);
    }
  }

  public void error(final String format, final Object... args) {
    if (isLoggable(Level.ERROR)) {
      logger.error(String.format(format, args));
    }
  }

  @Override
  public void warn(final Object message) {
    if (isLoggable(Level.WARN)) {
      logger.warn(message);
    }
  }

  @Override
  public void warn(final Object message, final Throwable t) {
    if (isLoggable(Level.WARN)) {
      logger.warn(message, t);
    }
  }

  public void warn(final String format, final Object... args) {
    if (isLoggable(Level.WARN)) {
      logger.warn(String.format(format, args));
    }
  }

  @Override
  public void info(final Object message) {
    if (isLoggable(Level.INFO)) {
      logger.info(message);
    }
  }

  @Override
  public void info(final Object message, final Throwable t) {
    if (isLoggable(Level.INFO)) {
      logger.info(message, t);
    }
  }

  public void info(final String format, final Object... args) {
    if (isLoggable(Level.INFO)) {
      logger.info(String.format(format, args));
    }
  }

  @Override
  public void debug(final Object message) {
    if (isLoggable(Level.DEBUG)) {
      logger.debug(message);
    }
  }

  @Override
  public void debug(final Object message, final Throwable t) {
    if (isLoggable(Level.DEBUG)) {
      logger.debug(message, t);
    }
  }

  public void debug(final String format, final Object... args) {
    if (isLoggable(Level.DEBUG)) {
      logger.debug(String.format(format, args));
    }
  }

  @Override
  public void trace(final Object message) {
    if (isLoggable(Level.DEBUG)) {
      logger.trace(message);
    }
  }

  @Override
  public void trace(final Object message, final Throwable t) {
    if (isLoggable(Level.DEBUG)) {
      logger.trace(message, t);
    }
  }

  public void trace(final String format, final Object... args) {
    if (isLoggable(Level.TRACE)) {
      logger.trace(String.format(format, args));
    }
  }

}
