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
package net.kuujo.vertigo.test.unit.cluster;

import static org.junit.Assert.*;
import net.kuujo.vertigo.cluster.Event;

import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An event test.
 *
 * @author Jordan Halterman
 */
public class EventTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testSerializeEvent() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("type", "timeout")
      .putString("key", "foo")
      .putString("value", "bar");
    Event event = mapper.readValue(serialized.encode(), Event.class);
    JsonObject json = new JsonObject(mapper.writeValueAsString(event));
    assertEquals("timeout", json.getString("type"));
    assertEquals("foo", json.getString("key"));
    assertEquals("bar", json.getString("value"));
  }

  @Test
  public void testDeserializeEvent() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("type", "timeout")
      .putString("key", "foo")
      .putString("value", "bar");
    Event event = mapper.readValue(serialized.encode(), Event.class);
    assertEquals(Event.Type.TIMEOUT, event.type());
    assertEquals("foo", event.key());
    assertEquals("bar", event.value());
  }

}
