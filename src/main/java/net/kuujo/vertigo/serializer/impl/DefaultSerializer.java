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
package net.kuujo.vertigo.serializer.impl;

import java.io.IOException;

import net.kuujo.vertigo.serializer.Serializable;
import net.kuujo.vertigo.serializer.SerializationException;
import net.kuujo.vertigo.serializer.Serializer;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A default serializer implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultSerializer implements Serializer {
  private final ObjectMapper mapper;

  public DefaultSerializer() {
    this(new InclusiveAnnotationIntrospector());
  }

  public DefaultSerializer(AnnotationIntrospector introspector) {
    mapper = new ObjectMapper();
    mapper.setAnnotationIntrospector(introspector);
  }

  @Override
  public JsonObject serialize(Serializable object) throws SerializationException {
    try {
      return new JsonObject(mapper.writeValueAsString(object));
    }
    catch (JsonProcessingException e) {
      throw new SerializationException(e.getMessage());
    }
  }

  @Override
  public <T extends Serializable> T deserialize(JsonObject serialized, Class<T> type)
      throws SerializationException {
    try {
      return mapper.readValue(serialized.encode(), type);
    }
    catch (IOException e) {
      throw new SerializationException(e.getMessage());
    }
  }

}