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
package net.kuujo.vertigo.cluster.impl;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

import net.kuujo.vertigo.cluster.Event;

/**
 * A default event implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultEvent implements Event {
  private Type type;
  private String key;
  private Object value;

  @Override
  public Type type() {
    return type;
  }

  @JsonGetter("type")
  private String getType() {
    return type.toString();
  }

  @JsonSetter("type")
  private void setType(String type) {
    this.type = Type.parse(type);
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T value() {
    return (T) value;
  }

}
