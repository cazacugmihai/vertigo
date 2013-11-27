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
package net.kuujo.vertigo.context;

import net.kuujo.vertigo.serializer.SerializationException;
import net.kuujo.vertigo.serializer.Serializer;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonBackReference;

/**
 * A component instance context.
 *
 * @author Jordan Halterman
 */
public final class InstanceContext {
  private String id;
  @SuppressWarnings("unused") private String address;
  @JsonBackReference private ComponentContext component;

  private InstanceContext() {
  }

  /**
   * Creates a new instance context from JSON.
   *
   * @param context
   *   A JSON representation of the instance context.
   * @return
   *   A new instance context instance.
   * @throws MalformedContextException
   *   If the JSON context is malformed.
   */
  public static InstanceContext fromJson(JsonObject context) throws MalformedContextException {
    try {
      return Serializer.getInstance().deserialize(context, InstanceContext.class);
    }
    catch (SerializationException e) {
      throw new MalformedContextException(e);
    }
  }

  /**
   * Sets the instance parent.
   */
  InstanceContext setParent(ComponentContext component) {
    this.component = component;
    return this;
  }

  /**
   * Returns the instance id.
   *
   * @return
   *   The unique instance id.
   */
  public String id() {
    return id;
  }

  /**
   * Returns the parent component context.
   *
   * @return
   *   The parent component context.
   */
  public ComponentContext getComponent() {
    return component;
  }

}
