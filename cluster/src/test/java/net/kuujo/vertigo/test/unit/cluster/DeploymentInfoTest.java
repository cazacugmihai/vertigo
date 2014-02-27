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
import net.kuujo.vertigo.cluster.AssignmentInfo;
import net.kuujo.vertigo.cluster.DeploymentInfo;
import net.kuujo.vertigo.cluster.InstanceInfo;
import net.kuujo.vertigo.cluster.ModuleDeploymentInfo;
import net.kuujo.vertigo.cluster.NodeInfo;
import net.kuujo.vertigo.cluster.VerticleDeploymentInfo;
import net.kuujo.vertigo.util.serializer.Serializer;
import net.kuujo.vertigo.util.serializer.SerializerFactory;

import org.junit.Test;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Deployment info test.
 *
 * @author Jordan Halterman
 */
public class DeploymentInfoTest {
  private Serializer serializer = SerializerFactory.getSerializer(DeploymentInfo.class);

  @Test
  public void testSerializeModuleDeploymentInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("id", "test")
      .putArray("targets", new JsonArray().add("foo").add("bar"))
      .putString("type", "module")
      .putString("module", "net.kuujo~mod-test~1.0")
      .putObject("config", new JsonObject().putString("foo", "bar"))
      .putNumber("instances", 2);
    DeploymentInfo info = serializer.deserializeFromObject(serialized, DeploymentInfo.class);
    JsonObject json = serializer.serializeToObject(info);
    assertEquals("test", json.getString("id"));
    assertTrue(json.getArray("targets").contains("foo"));
    assertTrue(json.getArray("targets").contains("bar"));
    assertEquals("module", json.getString("type"));
    assertEquals("net.kuujo~mod-test~1.0", json.getString("module"));
    assertEquals("bar", json.getObject("config").getString("foo"));
    assertTrue(json.getInteger("instances") == 2);
  }

  @Test
  public void testDeserializeModuleDeploymentInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("id", "test")
      .putArray("targets", new JsonArray().add("foo").add("bar"))
      .putString("type", "module")
      .putString("module", "net.kuujo~mod-test~1.0")
      .putObject("config", new JsonObject().putString("foo", "bar"))
      .putNumber("instances", 2);
    DeploymentInfo info = serializer.deserializeFromObject(serialized, DeploymentInfo.class);
    assertTrue(info instanceof ModuleDeploymentInfo);
    assertTrue(info.isModule());
    assertFalse(info.isVerticle());
    assertEquals("test", info.id());
    assertTrue(info.targets().contains("foo"));
    assertTrue(info.targets().contains("bar"));
    assertEquals("module", info.type().getName());
    assertEquals("net.kuujo~mod-test~1.0", ((ModuleDeploymentInfo) info).module());
    assertEquals("bar", info.config().getString("foo"));
    assertEquals(2, info.instances());
  }

  @Test
  public void testSerializeVerticleDeploymentInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("id", "test")
      .putArray("targets", new JsonArray().add("foo").add("bar"))
      .putString("type", "verticle")
      .putString("main", "test.js")
      .putObject("config", new JsonObject().putString("foo", "bar"))
      .putNumber("instances", 2);
    DeploymentInfo info = serializer.deserializeFromObject(serialized, DeploymentInfo.class);
    JsonObject json = serializer.serializeToObject(info);
    assertEquals("test", json.getString("id"));
    assertTrue(json.getArray("targets").contains("foo"));
    assertTrue(json.getArray("targets").contains("bar"));
    assertEquals("verticle", json.getString("type"));
    assertEquals("test.js", json.getString("main"));
    assertEquals("bar", json.getObject("config").getString("foo"));
    assertTrue(json.getInteger("instances") == 2);
  }

  @Test
  public void testDeserializeVerticleDeploymentInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("id", "test")
      .putArray("targets", new JsonArray().add("foo").add("bar"))
      .putString("type", "verticle")
      .putString("main", "test.js")
      .putObject("config", new JsonObject().putString("foo", "bar"))
      .putNumber("instances", 2)
      .putBoolean("worker", true)
      .putBoolean("multi-threaded", true);
    DeploymentInfo info = serializer.deserializeFromObject(serialized, DeploymentInfo.class);
    assertTrue(info instanceof VerticleDeploymentInfo);
    assertTrue(info.isVerticle());
    assertFalse(info.isModule());
    assertTrue(((VerticleDeploymentInfo) info).isWorker());
    assertTrue(((VerticleDeploymentInfo) info).isMultiThreaded());
    assertEquals("test", info.id());
    assertTrue(info.targets().contains("foo"));
    assertTrue(info.targets().contains("bar"));
    assertEquals("verticle", info.type().getName());
    assertEquals("test.js", ((VerticleDeploymentInfo) info).main());
    assertEquals("bar", info.config().getString("foo"));
    assertEquals(2, info.instances());
  }

  @Test
  public void testSerializeNodeInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("address", "test")
      .putArray("assignments", new JsonArray().add(new JsonObject()
        .putObject("instance", new JsonObject()
          .putString("id", "test1").putObject("deployment", new JsonObject()
            .putString("id", "deployment1")
            .putString("type", "verticle")
            .putString("main", "test.js")
            .putBoolean("worker", true))))
        .add(new JsonObject()
          .putObject("instance", new JsonObject()
            .putString("id", "test2").putObject("deployment", new JsonObject()
              .putString("id", "deployment2")
              .putString("type", "module")
              .putString("module", "net.kuujo~mod-test~1.0")))));
    NodeInfo info = serializer.deserializeFromObject(serialized, NodeInfo.class);
    JsonObject json = serializer.serializeToObject(info);
    assertEquals("test", json.getString("address"));
    assertTrue(json.getArray("assignments").isArray());
  }

  @Test
  public void testDeserializeNodeInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("address", "test")
      .putArray("assignments", new JsonArray().add(new JsonObject()
        .putObject("instance", new JsonObject()
          .putString("id", "test1").putObject("deployment", new JsonObject()
            .putString("id", "deployment1")
            .putString("type", "verticle")
            .putString("main", "test.js")
            .putBoolean("worker", true))))
        .add(new JsonObject()
          .putObject("instance", new JsonObject()
            .putString("id", "test2").putObject("deployment", new JsonObject()
              .putString("id", "deployment2")
              .putString("type", "module")
              .putString("module", "net.kuujo~mod-test~1.0")))));
    NodeInfo info = serializer.deserializeFromObject(serialized, NodeInfo.class);
    assertEquals("test", info.address());
    assertNotNull(info.assignments());
  }

  @Test
  public void testSerializeAssignmentInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putObject("instance", new JsonObject()
      .putString("id", "test1")
      .putObject("deployment", new JsonObject()
        .putString("id", "deployment1")
        .putString("type", "verticle")
        .putString("main", "test.js")
        .putBoolean("worker", true)));
    AssignmentInfo info = serializer.deserializeFromObject(serialized, AssignmentInfo.class);
    JsonObject json = serializer.serializeToObject(info);
    assertTrue(json.getObject("instance").isObject());
    assertEquals("test1", json.getObject("instance").getString("id"));
  }

  @Test
  public void testDeserializeAssignmentInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putObject("instance", new JsonObject()
      .putString("id", "test1")
      .putObject("deployment", new JsonObject()
        .putString("id", "deployment1")
        .putString("type", "verticle")
        .putString("main", "test.js")
        .putBoolean("worker", true)));
    AssignmentInfo info = serializer.deserializeFromObject(serialized, AssignmentInfo.class);
    assertNotNull(info.instance());
  }

  @Test
  public void testSerializeInstanceInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("id", "test")
      .putObject("deployment", new JsonObject()
        .putString("id", "deployment1")
        .putString("type", "verticle")
        .putString("main", "test.js")
        .putBoolean("worker", true));
    InstanceInfo info = serializer.deserializeFromObject(serialized, InstanceInfo.class);
    JsonObject json = serializer.serializeToObject(info);
    assertEquals("test", json.getString("id"));
    assertTrue(json.getObject("deployment").isObject());
    assertEquals("deployment1", json.getObject("deployment").getString("id"));
  }

  @Test
  public void testDeserializeInstanceInfo() throws Exception {
    JsonObject serialized = new JsonObject()
      .putString("id", "test")
      .putObject("deployment", new JsonObject()
        .putString("id", "deployment1")
        .putString("type", "verticle")
        .putString("main", "test.js")
        .putBoolean("worker", true));
    InstanceInfo info = serializer.deserializeFromObject(serialized, InstanceInfo.class);
    assertEquals("test", info.id());
    assertNotNull(info.deployment());
    assertEquals("deployment1", info.deployment().id());
  }

}
