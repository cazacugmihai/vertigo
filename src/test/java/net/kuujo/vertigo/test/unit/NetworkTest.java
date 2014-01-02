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

import net.kuujo.vertigo.feeder.Feeder;
import net.kuujo.vertigo.hooks.ComponentHook;
import net.kuujo.vertigo.input.Input;
import net.kuujo.vertigo.input.grouping.AllGrouping;
import net.kuujo.vertigo.input.grouping.FieldsGrouping;
import net.kuujo.vertigo.input.grouping.RandomGrouping;
import net.kuujo.vertigo.input.grouping.RoundGrouping;
import net.kuujo.vertigo.message.MessageId;
import net.kuujo.vertigo.network.Component;
import net.kuujo.vertigo.network.Config;
import net.kuujo.vertigo.network.Network;
import net.kuujo.vertigo.rpc.Executor;
import net.kuujo.vertigo.serializer.Serializer;
import net.kuujo.vertigo.serializer.SerializerFactory;
import net.kuujo.vertigo.worker.Worker;

import org.junit.Test;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Definition to context tests.
 *
 * @author Jordan Halterman
 */
public class NetworkTest {

  @Test
  public void testNetworkDefaults() {
    Network network = new Network("test");
    assertNotNull(network.getNetworkConfig());
    assertEquals("test", network.getAddress());
    assertEquals(0, network.getComponents().size());
    try {
      network.getComponent("foo");
      fail();
    }
    catch (IllegalArgumentException e) {}
  }

  @Test
  public void testConfigDefaults() {
    Config config = new Config();
    assertEquals("%1$s", config.getNetworkIdFormat());
    assertEquals(1, config.getNetworkNumAuditors());
    assertEquals(30000, config.getNetworkAckTimeout());
    assertEquals("%1$s-%3$s", config.getComponentIdFormat());
    assertEquals(1, config.getComponentDefaultNumInstances());
    assertEquals(new JsonObject(), config.getComponentDefaultConfig());
    assertEquals(5000, config.getComponentDefaultHeartbeatInterval());
    assertEquals("%1$s-%3$s-%7$d", config.getInstanceIdFormat());
  }

  @Test
  public void testNetworkConfig() {
    Network network = new Network("test");
    Config config = new Config();
    config.setNetworkAckTimeout(10000);
    assertEquals(10000, config.getNetworkAckTimeout());
    config.setNetworkNumAuditors(3);
    assertEquals(3, config.getNetworkNumAuditors());
    config.setNetworkIdFormat("%1$s");
    assertEquals("%1$s", config.getNetworkIdFormat());
    config.setComponentDefaultNumInstances(2);
    assertEquals(2, config.getComponentDefaultNumInstances());
    config.setComponentDefaultConfig(new JsonObject().putString("foo", "bar"));
    assertEquals("bar", config.getComponentDefaultConfig().getString("foo"));
    config.setComponentIdFormat("%1$s%3$s%4$s");
    assertEquals("%1$s%3$s%4$s", config.getComponentIdFormat());
    config.setInstanceIdFormat("%1$s%3$s%4$s%7$d");
    assertEquals("%1$s%3$s%4$s%7$d", config.getInstanceIdFormat());
    network.setNetworkConfig(config);
    assertEquals(10000, network.getNetworkConfig().getNetworkAckTimeout());
    assertEquals(3, network.getNetworkConfig().getNetworkNumAuditors());
    assertEquals(2, network.getNetworkConfig().getComponentDefaultNumInstances());
    assertEquals("bar", network.getNetworkConfig().getComponentDefaultConfig().getString("foo"));
  }

  @Test
  public void testAddFeederModule() {
    Network network = new Network("test");
    assertEquals("test", network.getAddress());
    Config config = new Config()
      .setComponentDefaultConfig(new JsonObject().putString("foo", "bar"))
      .setComponentDefaultNumInstances(2)
      .setComponentIdFormat("%1$s-%3$s-%4$s");
    network.setNetworkConfig(config);
    Component<Feeder> feeder = network.addFeeder("test.feeder", "com.test~test-feeder~1.0");
    assertEquals(Feeder.class, feeder.getType());
    assertEquals("test.feeder", feeder.getAddress());
    assertTrue(feeder.isModule());
    assertFalse(feeder.isVerticle());
    assertEquals("com.test~test-feeder~1.0", feeder.getModule());
    assertEquals("test-test.feeder-feeder", feeder.getComponentId());
    assertNull(feeder.getMain());
    assertEquals(2, feeder.getNumInstances());
    feeder.setNumInstances(1);
    assertEquals(1, feeder.getNumInstances());
    assertEquals("bar", feeder.getConfig().getString("foo"));
    feeder.setConfig(new JsonObject().putString("foo", "baz"));
    assertEquals("baz", feeder.getConfig().getString("foo"));
  }

  @Test
  public void testAddFeederVerticle() {
    Network network = new Network("test");
    assertEquals("test", network.getAddress());
    Config config = new Config()
      .setComponentDefaultConfig(new JsonObject().putString("foo", "bar"))
      .setComponentDefaultNumInstances(2);
    network.setNetworkConfig(config);
    Component<Feeder> feeder = network.addFeeder("test.feeder", "test_feeder.py");
    assertEquals(Feeder.class, feeder.getType());
    assertEquals("test.feeder", feeder.getAddress());
    assertFalse(feeder.isModule());
    assertTrue(feeder.isVerticle());
    assertEquals("test_feeder.py", feeder.getMain());
    assertNull(feeder.getModule());
    assertEquals(2, feeder.getNumInstances());
    feeder.setNumInstances(1);
    assertEquals(1, feeder.getNumInstances());
    assertEquals("bar", feeder.getConfig().getString("foo"));
    feeder.setConfig(new JsonObject().putString("foo", "baz"));
    assertEquals("baz", feeder.getConfig().getString("foo"));
  }

  @Test
  public void testAddExecutorModule() {
    Network network = new Network("test");
    assertEquals("test", network.getAddress());
    Config config = new Config()
      .setComponentDefaultConfig(new JsonObject().putString("foo", "bar"))
      .setComponentDefaultNumInstances(2);
    network.setNetworkConfig(config);
    Component<Executor> executor = network.addExecutor("test.executor", "com.test~test-executor~1.0");
    assertEquals(Executor.class, executor.getType());
    assertEquals("test.executor", executor.getAddress());
    assertTrue(executor.isModule());
    assertFalse(executor.isVerticle());
    assertEquals("com.test~test-executor~1.0", executor.getModule());
    assertNull(executor.getMain());
    assertEquals(2, executor.getNumInstances());
    executor.setNumInstances(1);
    assertEquals(1, executor.getNumInstances());
    assertEquals("bar", executor.getConfig().getString("foo"));
    executor.setConfig(new JsonObject().putString("foo", "baz"));
    assertEquals("baz", executor.getConfig().getString("foo"));
  }

  @Test
  public void testAddExecutorVerticle() {
    Network network = new Network("test");
    assertEquals("test", network.getAddress());
    Config config = new Config()
      .setComponentDefaultConfig(new JsonObject().putString("foo", "bar"))
      .setComponentDefaultNumInstances(2);
    network.setNetworkConfig(config);
    Component<Executor> executor = network.addExecutor("test.executor", "test_executor.py");
    assertEquals(Executor.class, executor.getType());
    assertEquals("test.executor", executor.getAddress());
    assertFalse(executor.isModule());
    assertTrue(executor.isVerticle());
    assertEquals("test_executor.py", executor.getMain());
    assertNull(executor.getModule());
    assertEquals(2, executor.getNumInstances());
    executor.setNumInstances(1);
    assertEquals(1, executor.getNumInstances());
    assertEquals("bar", executor.getConfig().getString("foo"));
    executor.setConfig(new JsonObject().putString("foo", "baz"));
    assertEquals("baz", executor.getConfig().getString("foo"));
  }

  @Test
  public void testAddWorkerModule() {
    Network network = new Network("test");
    assertEquals("test", network.getAddress());
    Config config = new Config()
      .setComponentDefaultConfig(new JsonObject().putString("foo", "bar"))
      .setComponentDefaultNumInstances(2);
    network.setNetworkConfig(config);
    Component<Worker> worker = network.addWorker("test.worker", "com.test~test-worker~1.0");
    assertEquals(Worker.class, worker.getType());
    assertEquals("test.worker", worker.getAddress());
    assertTrue(worker.isModule());
    assertFalse(worker.isVerticle());
    assertEquals("com.test~test-worker~1.0", worker.getModule());
    assertNull(worker.getMain());
    assertEquals(2, worker.getNumInstances());
    worker.setNumInstances(1);
    assertEquals(1, worker.getNumInstances());
    assertEquals("bar", worker.getConfig().getString("foo"));
    worker.setConfig(new JsonObject().putString("foo", "baz"));
    assertEquals("baz", worker.getConfig().getString("foo"));
  }

  @Test
  public void testAddWorkerVerticle() {
    Network network = new Network("test");
    assertEquals("test", network.getAddress());
    Config config = new Config()
      .setComponentDefaultConfig(new JsonObject().putString("foo", "bar"))
      .setComponentDefaultNumInstances(2);
    network.setNetworkConfig(config);
    Component<Worker> worker = network.addWorker("test.worker", "test_worker.py");
    assertEquals(Worker.class, worker.getType());
    assertEquals("test.worker", worker.getAddress());
    assertFalse(worker.isModule());
    assertTrue(worker.isVerticle());
    assertEquals("test_worker.py", worker.getMain());
    assertNull(worker.getModule());
    assertEquals(2, worker.getNumInstances());
    worker.setNumInstances(1);
    assertEquals(1, worker.getNumInstances());
    assertEquals("bar", worker.getConfig().getString("foo"));
    worker.setConfig(new JsonObject().putString("foo", "baz"));
    assertEquals("baz", worker.getConfig().getString("foo"));
  }

  @Test
  public void testAddInputs() {
    Network network = new Network("test");
    network.addFeeder("test.feeder", "test_feeder.py");

    Component<Worker> worker1 = network.addWorker("test.worker1", "test_worker1.py");
    Input input = worker1.addInput("test.feeder");
    assertEquals("test.feeder", input.getAddress());
    assertEquals("default", input.getStream());
    assertTrue(input.getGrouping() instanceof RoundGrouping);
    input.randomGrouping();
    assertTrue(input.getGrouping() instanceof RandomGrouping);
    input.allGrouping();
    assertTrue(input.getGrouping() instanceof AllGrouping);
    input.fieldsGrouping("foo", "bar");
    assertTrue(input.getGrouping() instanceof FieldsGrouping);
    assertEquals(2, ((FieldsGrouping) input.getGrouping()).getFields().size());
    assertTrue(((FieldsGrouping) input.getGrouping()).getFields().contains("foo"));
    assertTrue(((FieldsGrouping) input.getGrouping()).getFields().contains("bar"));
    Input input2 = worker1.addInput("test.feeder", "nondefault").randomGrouping();
    assertEquals("test.feeder", input2.getAddress());
    assertTrue(input2.getGrouping() instanceof RandomGrouping);
    input2.roundGrouping();
    assertTrue(input2.getGrouping() instanceof RoundGrouping);
    assertEquals("nondefault", input2.getStream());

    Component<Worker> worker2 = network.addWorker("test.worker2", "test_worker2.py", 2);
    worker2.addInput(worker1);
    assertEquals(1, worker2.getInputs().size());
    assertEquals("test.worker1", worker2.getInputs().get(0).getAddress());
  }

  @Test
  public void testAddHook() {
    Network network = new Network("test");
    Component<Feeder> feeder = network.addFeeder("test.feeder", "test_feeder.py");
    assertEquals(0, feeder.getHooks().size());
    feeder.addHook(new TestHook());
    assertEquals(1, feeder.getHooks().size());
    assertTrue(feeder.getHooks().get(0) instanceof TestHook);
  }

  @Test
  public void testSerializeNetwork() {
    Network network = new Network("test");
    network.getNetworkConfig().setNetworkNumAuditors(2);
    network.getNetworkConfig().setNetworkAckTimeout(10000);
    network.getNetworkConfig().setComponentDefaultConfig(new JsonObject().putString("foo", "bar"));
    network.getNetworkConfig().setComponentDefaultNumInstances(2);

    Serializer serializer = SerializerFactory.getSerializer(Network.class);
    JsonObject json = serializer.serialize(network);
    assertEquals("test", json.getString(Network.NETWORK_ADDRESS));
    assertEquals(0, json.getObject(Network.NETWORK_COMPONENTS).size());
    JsonObject config = json.getObject(Network.NETWORK_CONFIG);
    JsonObject networkConfig = config.getObject(Config.NETWORK);
    assertNotNull(networkConfig);
    assertTrue(networkConfig.getInteger(Config.NETWORK_NUM_AUDITORS) == 2);
    assertTrue(networkConfig.getLong(Config.NETWORK_ACK_TIMEOUT) == 10000);
    JsonObject componentConfig = config.getObject(Config.COMPONENTS);
    assertNotNull(componentConfig);
    assertEquals("bar", componentConfig.getObject(Config.COMPONENT_DEFAULT_CONFIG).getString("foo"));
    assertTrue(componentConfig.getInteger(Config.COMPONENT_DEFAULT_NUM_INSTANCES) == 2);

    Network result = serializer.deserialize(json, Network.class);
    assertEquals("test", result.getAddress());
    assertEquals(0, result.getComponents().size());
    assertNotNull(result.getNetworkConfig());
    assertEquals(2, result.getNetworkConfig().getNetworkNumAuditors());
    assertEquals(10000, result.getNetworkConfig().getNetworkAckTimeout());
    assertNotNull(result.getNetworkConfig().getComponentDefaultConfig());
    assertEquals("bar", result.getNetworkConfig().getComponentDefaultConfig().getString("foo"));
    assertEquals(2, result.getNetworkConfig().getComponentDefaultNumInstances());
  }

  @Test
  public void testSerializeNetworkWithComponents() {
    Network network = new Network("test");
    network.getNetworkConfig().setNetworkNumAuditors(2);
    network.getNetworkConfig().setNetworkAckTimeout(10000);
    network.getNetworkConfig().setComponentDefaultConfig(new JsonObject().putString("foo", "bar"));
    network.getNetworkConfig().setComponentDefaultNumInstances(2);

    Component<Feeder> feeder = network.addFeeder("test.feeder", "test_feeder.py");
    feeder.setConfig(new JsonObject().putString("bar", "baz"));
    feeder.setNumInstances(3);

    Component<Worker> worker = network.addWorker("test.worker", "com.test~test-worker~1.0");
    worker.addInput("test.feeder", "nondefault").allGrouping();
    worker.addHook(new TestHook());

    Serializer serializer = SerializerFactory.getSerializer(Network.class);
    JsonObject json = serializer.serialize(network);
    assertEquals("test", json.getString(Network.NETWORK_ADDRESS));
    JsonObject config = json.getObject(Network.NETWORK_CONFIG);
    JsonObject networkConfig = config.getObject(Config.NETWORK);
    assertNotNull(networkConfig);
    assertTrue(networkConfig.getInteger(Config.NETWORK_NUM_AUDITORS) == 2);
    assertTrue(networkConfig.getLong(Config.NETWORK_ACK_TIMEOUT) == 10000);
    JsonObject componentConfig = config.getObject(Config.COMPONENTS);
    assertNotNull(componentConfig);
    assertEquals("bar", componentConfig.getObject(Config.COMPONENT_DEFAULT_CONFIG).getString("foo"));
    assertTrue(componentConfig.getInteger(Config.COMPONENT_DEFAULT_NUM_INSTANCES) == 2);

    JsonObject jsonComponents = json.getObject(Network.NETWORK_COMPONENTS);
    assertEquals(2, jsonComponents.size());

    JsonObject jsonFeeder = jsonComponents.getObject("test.feeder");
    assertEquals("feeder", jsonFeeder.getString(Component.COMPONENT_TYPE));
    assertEquals("test.feeder", jsonFeeder.getString(Component.COMPONENT_ADDRESS));
    assertTrue(jsonFeeder.getInteger(Component.COMPONENT_NUM_INSTANCES) == 3);
    assertEquals("baz", jsonFeeder.getObject(Component.COMPONENT_CONFIG).getString("bar"));
    assertEquals(0, jsonFeeder.getArray(Component.COMPONENT_INPUTS).size());
    assertEquals(0, jsonFeeder.getArray(Component.COMPONENT_HOOKS).size());

    JsonObject jsonWorker = jsonComponents.getObject("test.worker");
    assertEquals("worker", jsonWorker.getString(Component.COMPONENT_TYPE));
    assertEquals("test.worker", jsonWorker.getString(Component.COMPONENT_ADDRESS));
    assertTrue(jsonWorker.getInteger(Component.COMPONENT_NUM_INSTANCES) == 2);
    assertEquals("bar", jsonWorker.getObject(Component.COMPONENT_CONFIG).getString("foo"));

    JsonArray jsonInputs = jsonWorker.getArray(Component.COMPONENT_INPUTS);
    assertEquals(1, jsonInputs.size());
    JsonObject jsonInput = jsonInputs.get(0);
    assertEquals("test.feeder", jsonInput.getString(Input.INPUT_ADDRESS));
    assertEquals("nondefault", jsonInput.getString(Input.INPUT_STREAM));
    assertEquals("all", jsonInput.getObject(Input.INPUT_GROUPING).getString("type"));

    JsonArray jsonHooks = jsonWorker.getArray(Component.COMPONENT_HOOKS);
    assertEquals(1, jsonHooks.size());
    JsonObject jsonHook = jsonHooks.get(0);
    assertEquals(TestHook.class.getName(), jsonHook.getString("type"));

    Network result = Network.fromJson(json);
    assertEquals("test", result.getAddress());
    assertNotNull(result.getNetworkConfig());
    assertEquals(2, result.getNetworkConfig().getNetworkNumAuditors());
    assertEquals(10000, result.getNetworkConfig().getNetworkAckTimeout());
    assertNotNull(result.getNetworkConfig().getComponentDefaultConfig());
    assertEquals("bar", result.getNetworkConfig().getComponentDefaultConfig().getString("foo"));
    assertEquals(2, result.getNetworkConfig().getComponentDefaultNumInstances());

    assertEquals(2, result.getComponents().size());
    feeder = result.getComponent("test.feeder");
    assertNotNull(feeder);
    assertFalse(feeder.isModule());
    assertTrue(feeder.isVerticle());
    assertEquals("test.feeder", feeder.getAddress());
    assertEquals(3, feeder.getNumInstances());
    assertEquals("baz", feeder.getConfig().getString("bar"));
    assertEquals(0, feeder.getInputs().size());
    assertEquals(0, feeder.getHooks().size());

    worker = result.getComponent("test.worker");
    assertNotNull(worker);
    assertTrue(worker.isModule());
    assertFalse(worker.isVerticle());
    assertEquals("test.worker", worker.getAddress());
    assertEquals(2, worker.getNumInstances());
    assertEquals("bar", worker.getConfig().getString("foo"));

    List<Input> inputs = worker.getInputs();
    assertEquals(1, inputs.size());
    Input input = inputs.get(0);
    assertEquals("test.feeder", input.getAddress());
    assertEquals("nondefault", input.getStream());
    assertTrue(input.getGrouping() instanceof AllGrouping);

    List<ComponentHook> hooks = worker.getHooks();
    assertEquals(1, hooks.size());
    ComponentHook hook = hooks.get(0);
    assertTrue(hook instanceof TestHook);
  }

  @Test
  public void testNetworkFromJson() {
    JsonObject json = new JsonObject();
    json.putString(Network.NETWORK_ADDRESS, "test");
    Network network = Network.fromJson(json);
    assertEquals("test", network.getAddress());
    assertNotNull(network.getNetworkConfig());
    assertNotNull(network.getComponents());
  }

  @Test
  public void testNetworkConfigFromJson() {
    JsonObject json = new JsonObject();
    json.putString(Network.NETWORK_ADDRESS, "test");

    JsonObject jsonConfig = new JsonObject();
    JsonObject networkConfig = new JsonObject();
    networkConfig.putNumber(Config.NETWORK_NUM_AUDITORS, 2);
    networkConfig.putNumber(Config.NETWORK_ACK_TIMEOUT, 10000);
    jsonConfig.putObject(Config.NETWORK, networkConfig);
    jsonConfig.putObject(Config.COMPONENTS, new JsonObject()
      .putObject(Config.COMPONENT_DEFAULT_CONFIG, new JsonObject().putString("foo", "bar"))
      .putNumber(Config.COMPONENT_DEFAULT_NUM_INSTANCES, 2));
    json.putObject(Network.NETWORK_CONFIG, jsonConfig);

    JsonObject jsonComponents = new JsonObject();
    json.putObject(Network.NETWORK_COMPONENTS, jsonComponents);

    Network network = Network.fromJson(json);
    assertEquals("test", network.getAddress());
    Config config = network.getNetworkConfig();
    assertEquals(2, config.getNetworkNumAuditors());
    assertEquals(10000, config.getNetworkAckTimeout());
    assertEquals("bar", config.getComponentDefaultConfig().getString("foo"));
    assertEquals(2, config.getComponentDefaultNumInstances());
  }

  @Test
  public void testNetworkComponentsFromJson() {
    JsonObject json = new JsonObject();
    json.putString(Network.NETWORK_ADDRESS, "test");

    JsonObject jsonComponents = new JsonObject();
    json.putObject(Network.NETWORK_COMPONENTS, jsonComponents);

    JsonObject jsonFeeder = new JsonObject();
    jsonFeeder.putString(Component.COMPONENT_ADDRESS, "test.feeder");
    jsonFeeder.putString(Component.COMPONENT_TYPE, "feeder");
    jsonFeeder.putString(Component.COMPONENT_MAIN, "test_feeder.py");
    jsonFeeder.putNumber(Component.COMPONENT_NUM_INSTANCES, 2);
    jsonComponents.putObject("test.feeder", jsonFeeder);

    JsonObject jsonWorker = new JsonObject();
    jsonWorker.putString(Component.COMPONENT_ADDRESS, "test.worker");
    jsonWorker.putString(Component.COMPONENT_TYPE, "worker");
    jsonWorker.putString(Component.COMPONENT_MODULE, "com.test~test-worker~1.0");
    jsonWorker.putNumber(Component.COMPONENT_NUM_INSTANCES, 2);

    JsonArray jsonInputs = new JsonArray();
    jsonInputs.add(new JsonObject().putString(Input.INPUT_ADDRESS, "test.feeder")
        .putObject(Input.INPUT_GROUPING, new JsonObject().putString("type", "fields")
            .putArray("fields", new JsonArray().add("foo").add("bar"))));
    jsonInputs.add(new JsonObject().putString(Input.INPUT_ADDRESS, "test.feeder")
        .putObject(Input.INPUT_GROUPING, new JsonObject().putString("type", "all"))
        .putString(Input.INPUT_STREAM, "nondefault"));
    jsonWorker.putArray(Component.COMPONENT_INPUTS, jsonInputs);

    jsonComponents.putObject("test.worker", jsonWorker);

    Network network = Network.fromJson(json);

    Component<Feeder> feeder = network.getComponent("test.feeder");
    assertEquals("test.feeder", feeder.getAddress());
    assertEquals(Feeder.class, feeder.getType());
    assertTrue(feeder.isVerticle());
    assertFalse(feeder.isModule());
    assertEquals("test_feeder.py", feeder.getMain());
    assertNull(feeder.getModule());
    assertEquals(2, feeder.getNumInstances());

    Component<Worker> worker = network.getComponent("test.worker");
    assertEquals("test.worker", worker.getAddress());
    assertEquals(Worker.class, worker.getType());
    assertTrue(worker.isModule());
    assertFalse(worker.isVerticle());
    assertEquals("com.test~test-worker~1.0", worker.getModule());
    assertNull(worker.getMain());
    assertEquals(2, worker.getNumInstances());

    List<Input> inputs = worker.getInputs();
    assertEquals(2, inputs.size());

    Input first = inputs.get(0);
    assertEquals("test.feeder", first.getAddress());
    assertTrue(first.getGrouping() instanceof FieldsGrouping);
    assertEquals(2, ((FieldsGrouping) first.getGrouping()).getFields().size());
    assertTrue(((FieldsGrouping) first.getGrouping()).getFields().contains("foo"));
    assertTrue(((FieldsGrouping) first.getGrouping()).getFields().contains("bar"));
    assertEquals("default", first.getStream());

    Input second = inputs.get(1);
    assertEquals("test.feeder", second.getAddress());
    assertTrue(second.getGrouping() instanceof AllGrouping);
    assertEquals("nondefault", second.getStream());
  }

  @Test
  public void testComponentHooksFromJson() {
    JsonObject json = new JsonObject();
    json.putString(Network.NETWORK_ADDRESS, "test");

    JsonObject jsonComponents = new JsonObject();
    json.putObject(Network.NETWORK_COMPONENTS, jsonComponents);

    JsonObject jsonWorker = new JsonObject();
    jsonWorker.putString(Component.COMPONENT_ADDRESS, "test.worker");
    jsonWorker.putString(Component.COMPONENT_TYPE, "worker");
    jsonWorker.putString(Component.COMPONENT_MODULE, "com.test~test-worker~1.0");
    jsonWorker.putNumber(Component.COMPONENT_NUM_INSTANCES, 2);

    JsonArray jsonInputs = new JsonArray();
    jsonInputs.add(new JsonObject().putString(Input.INPUT_ADDRESS, "test.feeder")
        .putObject(Input.INPUT_GROUPING, new JsonObject().putString("type", "fields")
            .putArray("fields", new JsonArray().add("foo").add("bar"))));
    jsonWorker.putArray(Component.COMPONENT_INPUTS, jsonInputs);

    JsonArray jsonHooks = new JsonArray();
    jsonHooks.add(new JsonObject().putString("type", TestHook.class.getName()));
    jsonWorker.putArray(Component.COMPONENT_HOOKS, jsonHooks);

    jsonComponents.putObject("test.worker", jsonWorker);

    Network network = Network.fromJson(json);

    assertEquals("test", network.getAddress());
    assertEquals(1, network.getComponents().size());
    Component<Worker> worker = network.getComponent("test.worker");
    assertEquals("test.worker", worker.getAddress());
    assertEquals(1, worker.getHooks().size());
    assertTrue(worker.getHooks().get(0) instanceof TestHook);
  }

  @Test
  public void testNetworkFromDeprecatedJson() {
    JsonObject json = new JsonObject();
    json.putString(Network.NETWORK_ADDRESS, "test");
    json.putNumber(Config.NETWORK_NUM_AUDITORS, 2);
    json.putNumber(Config.NETWORK_ACK_TIMEOUT, 10000);
    Network network = Network.fromJson(json);
    assertEquals(2, network.getNetworkConfig().getNetworkNumAuditors());
    assertEquals(10000, network.getNetworkConfig().getNetworkAckTimeout());
  }

  public static class TestHook implements ComponentHook {
    @Override
    public void handleStart(net.kuujo.vertigo.component.Component<?> subject) {
      
    }
    @Override
    public void handleStop(net.kuujo.vertigo.component.Component<?> subject) {
      
    }
    @Override
    public void handleReceive(MessageId messageId) {
      
    }
    @Override
    public void handleAck(MessageId messageId) {
      
    }
    @Override
    public void handleFail(MessageId messageId) {
      
    }
    @Override
    public void handleEmit(MessageId messageId) {
      
    }
    @Override
    public void handleAcked(MessageId messageId) {
      
    }
    @Override
    public void handleFailed(MessageId messageId) {
      
    }
    @Override
    public void handleTimeout(MessageId messageId) {
      
    }
  }

}
