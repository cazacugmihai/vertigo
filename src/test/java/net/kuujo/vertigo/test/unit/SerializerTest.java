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
package net.kuujo.vertigo.test.unit;

import net.kuujo.vertigo.context.ComponentContext;
import net.kuujo.vertigo.context.ContextBuilder;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.context.NetworkContext;
import net.kuujo.vertigo.context.VerticleContext;
import net.kuujo.vertigo.hooks.ComponentHook;
import net.kuujo.vertigo.hooks.EventBusHook;
import net.kuujo.vertigo.input.Input;
import net.kuujo.vertigo.input.grouping.RoundGrouping;
import net.kuujo.vertigo.network.MalformedNetworkException;
import net.kuujo.vertigo.network.Network;
import net.kuujo.vertigo.serializer.SerializationException;
import net.kuujo.vertigo.serializer.Serializer;

import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * A serializer test.
 *
 * @author Jordan Halterman
 */
public class SerializerTest {

  @Test
  public void testSerializeNetwork() {
    Network network = new Network("test");
    network.setNumAuditors(2);
    network.setAckTimeout(10000);
    network.addVerticle("1", "1.py", 2).addHook(new EventBusHook());
    network.addVerticle("2", "2.py", 2).addInput("1").groupBy(new RoundGrouping());

    Serializer serializer = Serializer.getInstance();
    try {
      JsonObject serialized = serializer.serialize(network);
      Network deserialized = serializer.deserialize(serialized, Network.class);
      assertTrue(deserialized.isAckingEnabled());
      assertEquals(10000, deserialized.getAckTimeout());
      assertEquals("test", deserialized.getAddress());
    }
    catch (SerializationException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testNetworkToContext() {
    Network network = new Network("test");
    network.setNumAuditors(2);
    network.setAckTimeout(10000);
    network.addVerticle("1", "1.py", 2).setConfig(new JsonObject().putString("foo", "bar")).addHook(new EventBusHook());
    network.addVerticle("2", "2.py", 2).addInput("1").groupBy(new RoundGrouping());

    try {
      NetworkContext context = ContextBuilder.buildContext(network);
      assertTrue(context.isAckingEnabled());
      assertEquals(10000, context.getAckTimeout());
      assertEquals("test", context.getAddress());
      assertEquals(2, context.getAuditors().size());
      ComponentContext component = context.getComponent("2");
      assertTrue(component instanceof VerticleContext);
      assertNotNull(component);
      assertEquals("2", component.getAddress());
      Input input = component.getInputs().get(0);
      assertNotNull(input);
      assertTrue(input.getGrouping() instanceof RoundGrouping);
      ComponentContext component2 = context.getComponent("1");
      assertTrue(component2 instanceof VerticleContext);
      ComponentHook hook = component2.getHooks().get(0);
      assertNotNull(hook);
      assertTrue(hook instanceof EventBusHook);
      InstanceContext instance = component2.getInstances().get(0);
      assertNotNull(instance);
      assertNotNull(instance.id());

      Serializer serializer = Serializer.getInstance();
      JsonObject serialized = serializer.serialize(context);
      serializer.deserialize(serialized, NetworkContext.class);
    }
    catch (MalformedNetworkException | SerializationException e) {
      fail(e.getMessage());
    }
  }

}
