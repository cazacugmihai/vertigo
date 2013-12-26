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

import java.util.List;

import net.kuujo.vertigo.context.ComponentContext;
import net.kuujo.vertigo.context.InputContext;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.context.NetworkContext;
import net.kuujo.vertigo.context.impl.ContextBuilder;
import net.kuujo.vertigo.feeder.Feeder;
import net.kuujo.vertigo.input.grouping.AllGrouping;
import net.kuujo.vertigo.input.grouping.FieldsGrouping;
import net.kuujo.vertigo.logging.Level;
import net.kuujo.vertigo.network.Component;
import net.kuujo.vertigo.network.Network;
import net.kuujo.vertigo.worker.Worker;

import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Definition to context tests.
 *
 * @author Jordan Halterman
 */
public class ContextTest {

  private NetworkContext createTestNetworkContext() {
    Network network = new Network("test");
    network.getConfig().setLogLevel(Level.DEBUG);
    network.getConfig().setAckTimeout(10000);
    network.getConfig().setNumAuditors(2);
    network.getConfig().setDefaultConfig(new JsonObject().putString("foo", "bar"));
    network.getConfig().setDefaultNumInstances(2);

    Component<Feeder> feeder = network.addFeeder("test.feeder", "test_feeder.js");
    feeder.setLogLevel(Level.WARN);
    feeder.setNumInstances(3);
    feeder.setConfig(new JsonObject().putString("bar", "baz"));

    Component<Worker> worker = network.addWorker("test.worker", "com.test~test-worker~1.0");
    worker.addInput("test.feeder").allGrouping();
    worker.addInput("test.feeder", "nondefault").fieldsGrouping("foo", "bar");

    return ContextBuilder.buildContext(network);
  }

  private void validateFullNetworkContext(NetworkContext networkContext) {
    validateTestNetworkContext(networkContext);
    validateTestFeederContext(networkContext.<Feeder>componentContext("test.feeder"));
    validateTestWorkerContext(networkContext.<Worker>componentContext("test.worker"));
  }

  private void validateTestNetworkContext(NetworkContext networkContext) {
    assertEquals("test", networkContext.address());
    assertNotNull(networkContext.config());
    assertEquals(Level.DEBUG, networkContext.config().logLevel());
    assertEquals(10000, networkContext.config().ackTimeout());
    assertEquals(2, networkContext.config().numAuditors());
    assertEquals("bar", networkContext.config().defaultConfig().getString("foo"));
    assertEquals(2, networkContext.config().defaultNumInstances());

    assertNotNull(networkContext.componentContexts());
    assertEquals(2, networkContext.componentContexts().size());

    try {
      networkContext.componentContext("invalid");
      fail("Invalid component found.");
    }
    catch (IllegalArgumentException e) {
    }

    ComponentContext<Feeder> feederContext = networkContext.componentContext("test.feeder");
    assertNotNull(feederContext);

    ComponentContext<Worker> workerContext = networkContext.componentContext("test.worker");
    assertNotNull(workerContext);
  }

  private void validateTestFeederContext(ComponentContext<Feeder> feederContext) {
    assertNotNull(feederContext);
    assertEquals("test.feeder", feederContext.address());
    assertFalse(feederContext.isModule());
    assertTrue(feederContext.isVerticle());
    assertEquals(Level.WARN, feederContext.logLevel());
    assertEquals("baz", feederContext.config().getString("bar"));
    assertEquals(3, feederContext.numInstances());
    assertEquals(3, feederContext.instanceContexts().size());
  }

  private void validateTestWorkerContext(ComponentContext<Worker> workerContext) {
    assertNotNull(workerContext);
    assertEquals("test.worker", workerContext.address());
    assertTrue(workerContext.isModule());
    assertFalse(workerContext.isVerticle());
    assertEquals(Level.DEBUG, workerContext.logLevel());
    assertEquals("bar", workerContext.config().getString("foo"));
    assertEquals(2, workerContext.numInstances());
    assertEquals(2, workerContext.instanceContexts().size());

    List<InputContext> inputContexts = workerContext.inputContexts();
    assertEquals(2, inputContexts.size());
    InputContext first = inputContexts.get(0);
    assertEquals("test.feeder", first.address());
    assertEquals("default", first.stream());
    assertEquals(2, first.count());
    assertTrue(first.grouping() instanceof AllGrouping);
    InputContext second = inputContexts.get(1);
    assertEquals("test.feeder", second.address());
    assertEquals("nondefault", second.stream());
    assertEquals(2, second.count());
    assertTrue(second.grouping() instanceof FieldsGrouping);
    assertTrue(((FieldsGrouping) second.grouping()).getFields().contains("foo"));
    assertTrue(((FieldsGrouping) second.grouping()).getFields().contains("bar"));
  }

  @Test
  public void testNetworkContext() {
    NetworkContext networkContext = createTestNetworkContext();
    validateFullNetworkContext(networkContext);
  }

  @Test
  public void testNetworkToFromJson() {
    NetworkContext networkContext = NetworkContext.fromJson(NetworkContext.toJson(createTestNetworkContext()));
    validateFullNetworkContext(networkContext);
  }

  @Test
  public void testComponentToFromJson() {
    NetworkContext networkContext = createTestNetworkContext();
    ComponentContext<Feeder> feederContext = ComponentContext.fromJson(ComponentContext.toJson(networkContext.componentContext("test.feeder")));
    validateTestFeederContext(feederContext);
    validateTestNetworkContext(feederContext.networkContext());
    ComponentContext<Worker> workerContext = ComponentContext.fromJson(ComponentContext.toJson(networkContext.componentContext("test.worker")));
    validateTestWorkerContext(workerContext);
    validateTestNetworkContext(workerContext.networkContext());
  }

  @Test
  public void testInstanceToFromJson() {
    NetworkContext networkContext = createTestNetworkContext();
    InstanceContext<Feeder> feederInstanceContext1 = InstanceContext.fromJson(InstanceContext.toJson(networkContext.componentContext("test.feeder").instanceContexts().get(0)));
    validateTestFeederContext(feederInstanceContext1.componentContext());
    validateTestNetworkContext(feederInstanceContext1.componentContext().networkContext());
    InstanceContext<Feeder> feederInstanceContext2 = InstanceContext.fromJson(InstanceContext.toJson(networkContext.componentContext("test.feeder").instanceContexts().get(1)));
    validateTestFeederContext(feederInstanceContext2.componentContext());
    validateTestNetworkContext(feederInstanceContext1.componentContext().networkContext());
    InstanceContext<Feeder> feederInstanceContext3 = InstanceContext.fromJson(InstanceContext.toJson(networkContext.componentContext("test.feeder").instanceContexts().get(2)));
    validateTestFeederContext(feederInstanceContext3.componentContext());
    validateTestNetworkContext(feederInstanceContext3.componentContext().networkContext());
    InstanceContext<Worker> workerInstanceContext1 = InstanceContext.fromJson(InstanceContext.toJson(networkContext.componentContext("test.worker").instanceContexts().get(0)));
    validateTestWorkerContext(workerInstanceContext1.componentContext());
    validateTestNetworkContext(workerInstanceContext1.componentContext().networkContext());
    InstanceContext<Worker> workerInstanceContext2 = InstanceContext.fromJson(InstanceContext.toJson(networkContext.componentContext("test.worker").instanceContexts().get(1)));
    validateTestWorkerContext(workerInstanceContext2.componentContext());
    validateTestNetworkContext(workerInstanceContext2.componentContext().networkContext());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInputToFromJson() {
    NetworkContext networkContext = createTestNetworkContext();
    InputContext inputContext1 = InputContext.fromJson(InputContext.toJson(networkContext.componentContext("test.worker").inputContexts().get(0)));
    assertEquals("test.feeder", inputContext1.address());
    assertEquals("default", inputContext1.stream());
    assertTrue(inputContext1.grouping() instanceof AllGrouping);
    assertEquals(2, inputContext1.count());
    validateTestWorkerContext((ComponentContext<Worker>) inputContext1.componentContext());
    validateTestNetworkContext(inputContext1.componentContext().networkContext());
    InputContext inputContext2 = InputContext.fromJson(InputContext.toJson(networkContext.componentContext("test.worker").inputContexts().get(1)));
    assertEquals("test.feeder", inputContext2.address());
    assertEquals("nondefault", inputContext2.stream());
    assertEquals(2, inputContext2.count());
    assertTrue(inputContext2.grouping() instanceof FieldsGrouping);
    assertTrue(((FieldsGrouping) inputContext2.grouping()).getFields().contains("foo"));
    assertTrue(((FieldsGrouping) inputContext2.grouping()).getFields().contains("bar"));
    validateTestWorkerContext((ComponentContext<Worker>) inputContext2.componentContext());
    validateTestNetworkContext(inputContext2.componentContext().networkContext());
  }

}
