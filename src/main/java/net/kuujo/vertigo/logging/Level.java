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
 * An internal logging level.
 *
 * @author Jordan Halterman
 */
public enum Level {
  TRACE("TRACE", 1),
  DEBUG("DEBUG", 2),
  INFO("INFO", 3),
  WARN("WARN", 4),
  ERROR("ERROR", 5),
  FATAL("FATAL", 6);

  /**
   * Parse a level string into a level.
   *
   * @param name
   *   The level name.
   * @return
   *   The matched level.
   */
  public static Level parse(String name) {
    switch (name) {
      case "TRACE":
        return TRACE;
      case "DEBUG":
        return DEBUG;
      case "INFO":
        return INFO;
      case "WARN":
        return WARN;
      case "ERROR":
        return ERROR;
      case "FATAL":
        return FATAL;
      default:
        throw new IllegalArgumentException("Invalid level name.");
    }
  }

  /**
   * Parse a level integer into a level.
   *
   * @param intValue
   *   The integer value.
   * @return
   *   The matched level.
   */
  public static Level parse(int intValue) {
    switch (intValue) {
      case 1:
        return TRACE;
      case 2:
        return DEBUG;
      case 3:
        return INFO;
      case 4:
        return WARN;
      case 5:
        return ERROR;
      case 6:
        return FATAL;
      default:
        throw new IllegalArgumentException("Invalid level value.");
    }
  }

  private final String name;
  private final int value;

  private Level(String name, int value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Returns the level name.
   */
  public final String getName() {
    return name;
  }

  /**
   * Returns the level's integer value.
   */
  public final int intValue() {
    return value;
  }

  @Override
  public String toString() {
    return getName();
  }

}
