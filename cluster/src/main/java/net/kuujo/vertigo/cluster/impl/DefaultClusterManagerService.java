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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Container;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.kuujo.vertigo.cluster.AssignmentInfo;
import net.kuujo.vertigo.cluster.ClusterManagerService;
import net.kuujo.vertigo.cluster.DeploymentException;
import net.kuujo.vertigo.cluster.DeploymentInfo;
import net.kuujo.vertigo.cluster.Event;
import net.kuujo.vertigo.cluster.InstanceInfo;
import net.kuujo.vertigo.cluster.ModuleDeploymentInfo;
import net.kuujo.vertigo.cluster.NodeInfo;
import net.kuujo.vertigo.cluster.StateMachine;
import net.kuujo.vertigo.cluster.VerticleDeploymentInfo;
import net.kuujo.vertigo.cluster.annotations.Command;
import net.kuujo.vertigo.cluster.annotations.Command.Argument;
import net.kuujo.vertigo.cluster.annotations.Stateful;
import net.kuujo.vertigo.cluster.config.ClusterConfig;
import net.kuujo.vertigo.cluster.state.StateContext;
import net.kuujo.vertigo.cluster.state.StateType;
import net.kuujo.vertigo.cluster.state.impl.DefaultStateMachineExecutor;
import net.kuujo.vertigo.util.serializer.Serializable;
import net.kuujo.vertigo.util.serializer.SerializationException;
import net.kuujo.vertigo.util.serializer.Serializer;
import net.kuujo.vertigo.util.serializer.SerializerFactory;

/**
 * Default cluster manager service implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultClusterManagerService implements ClusterManagerService, StateMachine {
  private final String address;
  private final String cluster;
  private final String broadcast;
  private final Vertx vertx;
  private final Container container;
  private final StateContext context;
  private final Logger logger = LoggerFactory.getLogger(DefaultClusterManagerService.class);
  private ClusterConfig config = new ClusterConfig();
  private long broadcastInterval = 2500;
  private boolean isLeader;
  @Stateful
  private Map<String, Set<Watcher>> watchers = new HashMap<>();
  @Stateful
  private Map<String, Value> data = new HashMap<>();
  @Stateful
  private Map<String, NodeReference> nodes = new HashMap<>();
  private Map<String, AssignmentInfoInternal> assignments = new HashMap<>();

  public DefaultClusterManagerService(String address, String cluster, Vertx vertx, Container container) {
    this.address = address;
    this.cluster = cluster;
    broadcast = String.format("%s.broadcast", cluster);
    this.vertx = vertx;
    this.container = container;
    context = new StateContext(UUID.randomUUID().toString(), vertx, new DefaultStateMachineExecutor(this));
  }

  private final Handler<StateType> transitionHandler = new Handler<StateType>() {
    @Override
    public void handle(StateType type) {
      if (type.equals(StateType.LEADER) && !isLeader) {
        isLeader = true;
        vertx.eventBus().registerHandler(address, clusterHandler, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            context.configure(config);
          }
        });
      }
      else if (!type.equals(StateType.LEADER) && isLeader) {
        vertx.eventBus().unregisterHandler(cluster, clusterHandler);
        isLeader = false;
      }
    }
  };

  // Handles cluster broadcast messages.
  private final Handler<Message<JsonObject>> broadcastHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "broadcast":
            doClusterBroadcast(message);
            break;
        }
      }
    }
  };

  // Handles messages sent to the cluster.
  private final Handler<Message<JsonObject>> clusterHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "deploy":
            doClusterDeploy(message);
            break;
          case "undeploy":
            doClusterUndeploy(message);
            break;
          case "set":
            doClusterSet(message);
            break;
          case "get":
            doClusterGet(message);
            break;
          case "delete":
            doClusterDelete(message);
            break;
          case "keys":
            doClusterKeys(message);
            break;
          case "exists":
            doClusterExists(message);
            break;
          case "timeout":
          case "reset":
            doClusterTimeout(message);
            break;
          case "cancel":
            doClusterCancel(message);
            break;
          case "watch":
            doClusterWatch(message);
            break;
          case "unwatch":
            doClusterUnwatch(message);
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  // Handles messages sent between nodes.
  private final Handler<Message<JsonObject>> internalHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "deploy":
            doNodeDeploy(message);
            break;
          case "assign":
            doInternalAssign(message);
            break;
          case "undeploy":
            doNodeUndeploy(message);
            break;
          case "unassign":
            doInternalUnassign(message);
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  @Override
  public String address() {
    return address;
  }

  @Override
  public String cluster() {
    return cluster;
  }

  @Override
  public ClusterManagerService start() {
    return start(null);
  }

  @Override
  public ClusterManagerService start(final Handler<AsyncResult<Void>> doneHandler) {
    vertx.setPeriodic(broadcastInterval, broadcastTimer);

    // Start the replica first. We need the replica to start and determine a
    // cluster leader before registering handlers on the event bus.
    context.start(new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        }
        else {
          // Register an internal (node-to-node) message handler.
          vertx.eventBus().registerHandler(address, internalHandler, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              }
              else {
                // Register a public broadcast message handler.
                vertx.eventBus().registerHandler(broadcast, broadcastHandler, new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    if (result.failed()) {
                      new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
                    }
                    else {
                      if (context.currentState().equals(StateType.LEADER)) {
                        isLeader = true;
                        vertx.eventBus().registerHandler(cluster, clusterHandler, new Handler<AsyncResult<Void>>() {
                          @Override
                          public void handle(AsyncResult<Void> result) {
                            if (result.failed()) {
                              new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
                            }
                            else {
                              addNode(address, context.address());
                              context.transitionHandler(transitionHandler);
                              context.configure(config);
                              new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                            }
                          }
                        });
                      }
                      else {
                        context.transitionHandler(transitionHandler);
                        new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                      }
                    }
                  }
                });
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public void stop() {
    context.stop();
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    context.stop(doneHandler);
  }

  // A periodic broadcast timer. Broadcasting is used to notify other nodes
  // within the cluster of this node's existence.
  private final Handler<Long> broadcastTimer = new Handler<Long>() {
    @Override
    public void handle(Long timerID) {
      vertx.eventBus().publish(broadcast, new JsonObject()
          .putString("action", "broadcast")
          .putString("address", address)
          .putString("replica", context.address()));
    }
  };

  // A node broadcast timeout handler. This handler is triggered when a node
  // has not broadcasted its presence for some period of time.
  private final Handler<String> timeoutHandler = new Handler<String>() {
    @Override
    public void handle(String address) {
      if (isLeader && nodes.containsKey(address)) {
        final NodeReference node = nodes.get(address);
        context.submitCommand("removeNode", new JsonObject().putString("address", address), new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            reassignNodeDeployments(node);
          }
        });
      }
    }
  };

  @Stateful("watchers")
  public void setWatchers(Map<String, List<Map<String, Object>>> watchers) {
    this.watchers = new HashMap<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : watchers.entrySet()) {
      Set<Watcher> watchersSet = new HashSet<>();
      for (Map<String, Object> watcher : entry.getValue()) {
        watchersSet.add(new Watcher(watcher.get("address").toString(), watcher.containsKey("event") ? Event.Type.parse(watcher.get("event").toString()) : null));
      }
      this.watchers.put(entry.getKey(), watchersSet);
    }
  }

  @Stateful("data")
  public void setData(Map<String, Map<String, Object>> data) {
    this.data = new HashMap<>();
    for (Map.Entry<String, Map<String, Object>> entry : data.entrySet()) {
      this.data.put(entry.getKey(), new Value(entry.getValue().get("value"), entry.getValue().containsKey("expire") ? ((Number) entry.getValue().get("expire")).longValue() : 0));
    }
  }

  @Stateful("nodes")
  public void setNodes(Map<String, Map<String, Object>> nodes) {
    Serializer serializer = SerializerFactory.getSerializer(NodeReference.class);
    this.nodes = new HashMap<>();
    for (Map.Entry<String, Map<String, Object>> entry : nodes.entrySet()) {
      try {
        this.nodes.put(entry.getKey(), serializer.deserializeFromString(serializer.serializeToString((Serializable) entry.getValue()), NodeReference.class));
      }
      catch (SerializationException e) {
        continue;
      }
    }
  }

  /**
   * A state machine command for adding a node to the cluster.
   * 
   * @param address The node address.
   * @param id The node ID.
   * @param replica The node's replica address.
   */
  @Command(type = Command.Type.WRITE)
  public void addNode(@Argument("address") String address, @Argument("replica") String replica) {
    NodeReference node;
    if (!nodes.containsKey(address)) {
      node = new NodeReference(address, replica).timeoutHandler(timeoutHandler);
      nodes.put(address, node);
    }
    else {
      node = nodes.get(address);
    }

    node.replica = replica;
    node.resetTimer();
  }

  /**
   * A state machine command for updating a node's deployments and info.
   * 
   * @param address The node address.
   * @param id The node ID.
   * @param replica The node's replica address.
   * @param action The update action.
   * @param info The node's info.
   * @return Indicates whether the node was successfully updated.
   */
  @Command(type = Command.Type.WRITE)
  public boolean updateNode(@Argument("address") String address, @Argument("id") String id, @Argument("replica") String replica,
      @Argument(value = "info", required = false) NodeInfo info) {
    if (!nodes.containsKey(address)) {
      return false;
    }

    NodeReference node = nodes.get(address);
    node.replica = replica;

    if (info != null)
      node.info = info;
    return true;
  }

  /**
   * A state machine command for removing a node from the cluster.
   * 
   * @param address The node address.
   * @return Indicates whether the node was removed.
   */
  @Command(type = Command.Type.WRITE)
  public boolean removeNode(@Argument("address") String address) {
    if (nodes.containsKey(address)) {
      nodes.remove(address);
      return true;
    }
    return false;
  }

  /**
   * A data store key.
   */
  private static class Value {
    @JsonProperty
    private Object value;
    @JsonProperty
    private long expire;
    @JsonIgnore
    private long timer;

    private Value(Object value) {
      this(value, 0);
    }

    private Value(Object value, long expire) {
      this.value = value;
      if (expire > 0) {
        this.expire = System.currentTimeMillis() + expire;
      }
      else {
        this.expire = 0;
      }
    }
  }

  /**
   * A key watcher.
   */
  private static class Watcher implements Serializable {
    private final String address;
    private final Event.Type event;

    private Watcher(String address) {
      this.address = address;
      this.event = null;
    }

    private Watcher(String address, Event.Type event) {
      this.address = address;
      this.event = event;
    }

    @Override
    public boolean equals(Object obj) {
      return obj.equals(address) || (obj instanceof Watcher && ((Watcher) obj).address.equals(address));
    }
  }

  /**
   * A simple set command.
   * 
   * @param key The key to set.
   * @param value The value to set.
   * @param expire An optional key expiration.
   */
  @Command(type = Command.Type.WRITE)
  public void set(@Argument("key") String key, @Argument("value") Object value, @Argument(value = "expire", required = false) long expire) {
    if (!data.containsKey(key)) {
      data.put(key, new Value(value, expire));
      if (isLeader) {
        triggerEvent("create", key, value);
      }
    }
    else {
      data.put(key, new Value(value, expire));
      if (isLeader) {
        triggerEvent("update", key, value);
      }
    }
  }

  /**
   * A simple get command.
   * 
   * @param key The key to get.
   * @param defaultValue An optional default value.
   * @return The key value or the default if none was found.
   */
  @Command(type = Command.Type.READ)
  public Object get(@Argument("key") String key, @Argument(value = "default", required = false) Object defaultValue) {
    Value value = data.get(key);
    if (value != null) {
      if (value.expire > 0 && value.expire < System.currentTimeMillis()) {
        data.remove(key);

        if (isLeader) {
          triggerEvent("delete", key, value.value);
        }
        return null;
      }
      return value.value;
    }
    return defaultValue;
  }

  /**
   * A simple delete command.
   * 
   * @param key The key to delete.
   * @return Indicates whether the key was deleted.
   */
  @Command(type = Command.Type.WRITE)
  public boolean delete(@Argument("key") String key) {
    if (data.containsKey(key)) {
      Value value = data.remove(key);

      if (isLeader) {
        triggerEvent("delete", key, value.value);
      }
      return true;
    }
    return false;
  }

  /**
   * A simple keys command.
   * 
   * @return A list of keys.
   */
  @Command(type = Command.Type.READ)
  public List<String> keys() {
    List<String> keys = new ArrayList<>();
    Iterator<Map.Entry<String, Value>> iterator = data.entrySet().iterator();
    long currentTime = System.currentTimeMillis();

    while (iterator.hasNext()) {
      // Don't actually remove the key from the map when looking up keys because
      // this command is not replicated to the rest of the cluster, and thus
      // the key would only be removed on this node.
      Map.Entry<String, Value> entry = iterator.next();
      if (entry.getValue().expire < currentTime) {
        keys.add(entry.getKey());
      }
    }
    return keys;
  }

  /**
   * A simple exists command.
   * 
   * @param key The key to check.
   * @return Indicates whether the key exists.
   */
  @Command(type = Command.Type.READ)
  public boolean exists(@Argument("key") String key) {
    return data.containsKey(key);
  }

  /**
   * A simple key watch command.
   * 
   * @param key The key to watch.
   * @param address The address at which to watch.
   * @return Indicates whether the address began watching the key.
   */
  @Command(type = Command.Type.WRITE)
  public boolean watch(@Argument("key") String key, @Argument("address") String address,
      @Argument(value = "event", required = false) String event) {
    Set<Watcher> watch;
    if (!watchers.containsKey(key)) {
      watch = new HashSet<>();
      watchers.put(key, watch);
    }
    else {
      watch = watchers.get(key);
    }
    return watch.add(new Watcher(address, event != null ? Event.Type.parse(event) : null));
  }

  /**
   * A simple key unwatch command.
   * 
   * @param key The key to unwatch.
   * @param address The address at which to unwatch.
   * @return Indicates whether the address stopped watching the key.
   */
  @Command(type = Command.Type.WRITE)
  public boolean unwatch(@Argument("key") String key, @Argument("address") String address,
      @Argument(value = "event", required = false) String event) {
    if (!watchers.containsKey(key)) {
      return false;
    }

    Set<Watcher> watch = watchers.get(key);
    Event.Type watchEvent = event != null ? Event.Type.parse(event) : null;
    Iterator<Watcher> iterator = watch.iterator();
    boolean removed = false;
    while (iterator.hasNext()) {
      Watcher watcher = iterator.next();
      if (watcher.address.equals(address) && (watchEvent == null || (watcher.event != null && watcher.event.equals(watchEvent)))) {
        iterator.remove();
        removed = true;
      }
    }
    if (watch.isEmpty()) {
      watchers.remove(key);
    }
    return removed;
  }

  /**
   * Handles cluster broadcasting.
   * 
   * If the current node is the cluster leader, the cluster membership configuration may
   * be updated by the leader. In that case, the underlying replication system will log
   * the configuration change and replicate the new configuration to the rest of the
   * cluster.
   */
  private void doClusterBroadcast(final Message<JsonObject> message) {
    if (isLeader) {
      final String address = message.body().getString("address");
      final String replica = message.body().getString("replica");
      if (!nodes.containsKey(address)) {
        if (!config.containsMember(replica)) {
          config.addMember(replica);
        }
        this.context.submitCommand("addNode", message.body(), new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (nodes.containsKey(address)) {
              nodes.get(address).resetTimer();
            }
          }
        });
      }
      else if (!nodes.get(address).replica.equals(replica)) {
        this.context.submitCommand("updateNode", message.body(), new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            if (nodes.containsKey(address)) {
              nodes.get(address).resetTimer();
            }
          }
        });
      }
      else {
        nodes.get(address).resetTimer();
      }
    }
  }

  /**
   * Called when a set command is received.
   */
  private void doClusterSet(final Message<JsonObject> message) {
    context.submitCommand("set", message.body(), new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  /**
   * Called when a get command is received.
   */
  private void doClusterGet(final Message<JsonObject> message) {
    context.submitCommand("get", message.body(), new Handler<AsyncResult<Object>>() {
      @Override
      public void handle(AsyncResult<Object> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else if (result.result() == null) {
          message.reply(new JsonObject().putString("status", "ok"));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putValue("result", result.result()));
        }
      }
    });
  }

  /**
   * Called when a delete command is received.
   */
  private void doClusterDelete(final Message<JsonObject> message) {
    context.submitCommand("delete", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Called when a keys command is received.
   */
  private void doClusterKeys(final Message<JsonObject> message) {
    context.submitCommand("keys", message.body(), new Handler<AsyncResult<List<String>>>() {
      @Override
      public void handle(AsyncResult<List<String>> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putArray("result", new JsonArray(result.result().toArray(new Object[result.result().size()]))));
        }
      }
    });
  }

  /**
   * Called when a exists command is received.
   */
  private void doClusterExists(final Message<JsonObject> message) {
    context.submitCommand("exists", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Sets a timeout.
   */
  private void doClusterTimeout(final Message<JsonObject> message) {
    context.submitCommand("timeout", message.body(), new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  /**
   * Cancels a timer.
   */
  private void doClusterCancel(final Message<JsonObject> message) {
    context.submitCommand("cancel", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Watches a key.
   */
  private void doClusterWatch(final Message<JsonObject> message) {
    context.submitCommand("watch", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Unwatches a key.
   */
  private void doClusterUnwatch(final Message<JsonObject> message) {
    context.submitCommand("unwatch", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Triggers an event.
   */
  private void triggerEvent(String event, String key, Object value) {
    if (watchers.containsKey(key)) {
      Event.Type watchEvent = Event.Type.parse(event);
      for (Watcher watcher : watchers.get(key)) {
        if (watcher.event == null || watcher.event.equals(watchEvent)) {
          triggerEvent(watcher.address, event, key, value);
        }
      }
    }
  }

  /**
   * Triggers an event to a specific subscriber.
   */
  private void triggerEvent(String address, String event, String key, Object value) {
    vertx.eventBus().send(address, new JsonObject().putString("type", event).putString("key", key).putValue("value", value));
  }

  /**
   * Called when the node receives a deploy message. The cluster leader is the only node
   * that actually handles deployments, so the message is simply forwarded on to the
   * leader.
   */
  private void doNodeDeploy(final Message<JsonObject> message) {
    message.body().putString("target", address);
    doClusterDeploy(message);
  }

  /**
   * Called when the node receives an undeploy message. The cluster leader is the only
   * node that actually handles deployments, so the message is simply forwarded on to the
   * leader.
   */
  private void doNodeUndeploy(final Message<JsonObject> message) {
    message.body().putString("target", address);
    doClusterUndeploy(message);
  }

  /**
   * Called when the node receives a deploy message. If this node received a cluster
   * deploy message then it must have been the cluster leader.
   */
  private void doClusterDeploy(final Message<JsonObject> message) {
    if (!isLeader) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Internal error. Not the leader"));
      return;
    }

    DeploymentInfo deploymentInfo;
    Serializer serializer = SerializerFactory.getSerializer(DeploymentInfo.class);
    try {
      deploymentInfo = serializer.deserializeFromObject(message.body(), DeploymentInfo.class);
    }
    catch (SerializationException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
      return;
    }

    final String id = deploymentInfo.id();
    doDeployInstances(deploymentInfo, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putString("id", id));
        }
      }
    });
  }

  /**
   * Deploys instances separately across the cluster.
   */
  private void doDeployInstances(final DeploymentInfo deploymentInfo, final Handler<AsyncResult<Void>> doneHandler) {
    List<InstanceInfo> instances = new ArrayList<>();
    for (int i = 0; i < deploymentInfo.instances(); i++) {
      instances.add(InstanceInfo.Builder.newBuilder().setId(UUID.randomUUID().toString()).setDeployment(deploymentInfo).build());
    }
    doDeployInstances(0, instances, deploymentInfo, doneHandler);
  }

  /**
   * Deploys instances separately across the cluster.
   */
  private void doDeployInstances(final int instance, final List<InstanceInfo> instances, final DeploymentInfo deploymentInfo,
      final Handler<AsyncResult<Void>> doneHandler) {
    // If a deployment target was specified then attempt to assign
    // the deployment to that target. If the target doesn't exist
    // then fail the deployment.
    if (!deploymentInfo.targets().isEmpty()) {
      Map<String, NodeReference> nodes = new HashMap<>();
      for (String target : deploymentInfo.targets()) {
        if (this.nodes.containsKey(target)) {
          nodes.put(target, this.nodes.get(target));
        }
      }
      if (nodes.isEmpty()) {
        new DefaultFutureResult<Void>(new DeploymentException("Invalid deployment targets")).setHandler(doneHandler);
      }
      else {
        doDeployInstances(nodes, instance, instances, deploymentInfo, doneHandler);
      }
    }
    else {
      doDeployInstances(new HashMap<>(nodes), instance, instances, deploymentInfo, doneHandler);
    }
  }

  /**
   * Deploys instances recursively from a set of nodes.
   */
  private void doDeployInstances(final Map<String, NodeReference> nodes, final int instance, final List<InstanceInfo> instances,
      final DeploymentInfo deploymentInfo, final Handler<AsyncResult<Void>> doneHandler) {
    if (instance < instances.size()) {
      final NodeReference node = pickNode(nodes);
      if (node == null) {
        new DefaultFutureResult<Void>().setHandler(doneHandler)
            .setFailure(new DeploymentException("Failed to assign deployment to any node."));
      }
      else {
        final AssignmentInfo assignment = AssignmentInfo.Builder.newBuilder().setInstance(instances.get(instance)).build();
        Serializer serializer = SerializerFactory.getSerializer(AssignmentInfo.class);
        try {
          vertx.eventBus().sendWithTimeout(node.address, serializer.serializeToObject(assignment).putString("action", "assign"), 15000,
              new Handler<AsyncResult<Message<JsonObject>>>() {
                @Override
                public void handle(AsyncResult<Message<JsonObject>> result) {
                  if (result.failed()) {
                    if (nodes.size() > 0) {
                      doDeployInstances(nodes, instance, instances, deploymentInfo, doneHandler);
                    }
                    else {
                      new DefaultFutureResult<Void>().setHandler(doneHandler).setFailure(result.cause());
                    }
                  }
                  else if (result.result().body().getString("status").equals("error")) {
                    if (nodes.size() > 0) {
                      doDeployInstances(nodes, instance, instances, deploymentInfo, doneHandler);
                    }
                    else {
                      new DefaultFutureResult<Void>().setHandler(doneHandler).setFailure(
                          new DeploymentException(result.result().body().getString("message")));
                    }
                  }
                  else {
                    Serializer serializer = SerializerFactory.getSerializer(NodeInfo.class);
                    try {
                      node.info = NodeInfo.Builder.newBuilder(node.info).addAssignment(assignment).build();
                      context.submitCommand("updateNode", new JsonObject().putString("replica", node.replica)
                          .putString("address", node.address).putObject("info", serializer.serializeToObject(node.info)), new Handler<AsyncResult<Boolean>>() {
                        @Override
                        public void handle(AsyncResult<Boolean> result) {
                          if (result.failed()) {
                            new DefaultFutureResult<Void>().setHandler(doneHandler).setFailure(result.cause());
                          }
                          else {
                            doDeployInstances(instance + 1, instances, deploymentInfo, doneHandler);
                          }
                        }
                      });
                    }
                    catch (SerializationException e) {
                      new DefaultFutureResult<Void>(e).setHandler(doneHandler);
                    }
                  }
                }
              });
        }
        catch (SerializationException e) {
          new DefaultFutureResult<Void>(e).setHandler(doneHandler);
        }
      }
    }
    else {
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
    }
  }

  /**
   * Handles assignment of a deployment to the node.
   */
  private void doInternalAssign(final Message<JsonObject> message) {
    AssignmentInfo info;
    Serializer serializer = SerializerFactory.getSerializer(AssignmentInfo.class);
    try {
      info = serializer.deserializeFromObject(message.body(), AssignmentInfo.class);
    }
    catch (SerializationException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
      return;
    }

    final String id = info.instance().id();
    if (info.instance().deployment().type().equals(DeploymentInfo.Type.MODULE)) {
      ModuleDeploymentInfo deployment = (ModuleDeploymentInfo) info.instance().deployment();
      container.deployModule(deployment.module(), deployment.config(), 1, new Handler<AsyncResult<String>>() {
        @Override
        public void handle(AsyncResult<String> result) {
          if (result.failed()) {
            message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
          }
          else {
            assignments.put(id, new AssignmentInfoInternal(result.result(), AssignmentInfoInternal.MODULE));
            message.reply(new JsonObject().putString("status", "ok").putString("id", id));
          }
        }
      });
    }
    else if (info.instance().deployment().type().equals(DeploymentInfo.Type.VERTICLE)) {
      VerticleDeploymentInfo deployment = (VerticleDeploymentInfo) info.instance().deployment();
      if (deployment.isWorker()) {
        container.deployWorkerVerticle(deployment.main(), deployment.config(), 1, deployment.isMultiThreaded(),
            new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                if (result.failed()) {
                  message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
                }
                else {
                  assignments.put(id, new AssignmentInfoInternal(result.result(), AssignmentInfoInternal.VERTICLE));
                  message.reply(new JsonObject().putString("status", "ok").putString("id", id));
                }
              }
            });
      }
      else {
        container.deployVerticle(deployment.main(), deployment.config(), new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            if (result.failed()) {
              message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
            }
            else {
              assignments.put(id, new AssignmentInfoInternal(result.result(), AssignmentInfoInternal.VERTICLE));
              message.reply(new JsonObject().putString("status", "ok").putString("id", id));
            }
          }
        });
      }
    }
    else {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment type."));
    }
  }

  /**
   * Handles an internal undeploy message.
   */
  private void doClusterUndeploy(final Message<JsonObject> message) {
    if (!isLeader) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Internal error. Not the leader"));
      return;
    }

    final String parentID = message.body().getString("id");
    Set<String> targets = null;
    if (message.body().containsField("target")) {
      targets = new HashSet<>();
      targets.add(message.body().getString("target"));
    }
    else if (message.body().containsField("targets")) {
      targets = new HashSet<>();
      for (Object target : message.body().getArray("targets")) {
        targets.add((String) target);
      }
    }

    doUndeployInstances(parentID, targets, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  /**
   * Undeploys all instances related to a deployment ID.
   */
  private void doUndeployInstances(final String parentID, final Set<String> targets, final Handler<AsyncResult<Void>> doneHandler) {
    // If a target was defined then only undeploy the instance(s) related
    // to that specific target node.
    if (targets != null) {
      Map<String, NodeReference> nodes = new HashMap<>();
      for (String target : targets) {
        if (this.nodes.containsKey(target)) {
          nodes.put(target, this.nodes.get(target));
        }
      }
      if (nodes.isEmpty()) {
        new DefaultFutureResult<Void>(new DeploymentException("Invalid deployment target")).setHandler(doneHandler);
      }
      else {
        doUndeployInstances(parentID, nodes, doneHandler);
      }
    }
    else {
      doUndeployInstances(parentID, this.nodes, doneHandler);
    }
  }

  /**
   * Undeploys instances from a set of nodes.
   */
  private void doUndeployInstances(final String parentID, final Map<String, NodeReference> nodes,
      final Handler<AsyncResult<Void>> doneHandler) {
    final List<NodeReference> nodeList = new ArrayList<>();
    final List<AssignmentInfo> assignmentList = new ArrayList<>();
    for (NodeReference node : nodes.values()) {
      for (AssignmentInfo assignment : node.info.assignments()) {
        if (assignment.instance().deployment().id().equals(parentID)) {
          nodeList.add(node);
          assignmentList.add(assignment);
        }
      }
    }

    // If no instances were found then the deployment doesn't exist.
    if (assignmentList.isEmpty()) {
      new DefaultFutureResult<Void>(new DeploymentException("Invalid deployment ID.")).setHandler(doneHandler);
    }
    else {
      doUndeployInstances(0, nodeList, assignmentList, null, doneHandler);
    }
  }

  /**
   * Recursively undeploys instances from a list of instances to be undeployed.
   */
  private void doUndeployInstances(final int current, final List<NodeReference> nodeList, final List<AssignmentInfo> assignmentList,
      final Throwable t, final Handler<AsyncResult<Void>> doneHandler) {
    if (current < nodeList.size()) {
      final NodeReference node = nodeList.get(current);
      final AssignmentInfo assignment = assignmentList.get(current);
      vertx.eventBus().sendWithTimeout(node.address, new JsonObject().putString("action", "unassign").putString("id", assignment.instance().id()), 15000,
          new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(final AsyncResult<Message<JsonObject>> result) {
              NodeInfo nodeInfo = NodeInfo.Builder.newBuilder(node.info).removeAssignment(assignment).build();
              Serializer serializer = SerializerFactory.getSerializer(NodeInfo.class);
              try {
                context.submitCommand("updateNode",
                    new JsonObject().putString("address", node.address).putString("replica", node.replica)
                        .putString("action", "unassign").putObject("info", serializer.serializeToObject(nodeInfo)),
                    new Handler<AsyncResult<Boolean>>() {
                      @Override
                      public void handle(AsyncResult<Boolean> commandResult) {
                        if (result.failed()) {
                          doUndeployInstances(current + 1, nodeList, assignmentList, t != null ? t : result.cause(), doneHandler);
                        }
                        else if (result.result().body().getString("status").equals("error")) {
                          doUndeployInstances(current + 1, nodeList, assignmentList, t != null ? t : new DeploymentException(result
                              .result().body().getString("message")), doneHandler);
                        }
                        else if (commandResult.failed()) {
                          doUndeployInstances(current + 1, nodeList, assignmentList, t != null ? t : commandResult.cause(), doneHandler);
                        }
                        else {
                          doUndeployInstances(current + 1, nodeList, assignmentList, t, doneHandler);
                        }
                      }
                    });
              }
              catch (SerializationException e) {
                doUndeployInstances(current + 1, nodeList, assignmentList, t != null ? t : e, doneHandler);
              }
            }
          });
    }
    else if (t != null) {
      new DefaultFutureResult<Void>(t).setHandler(doneHandler);
    }
    else {
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
    }
  }

  /**
   * Handles the unassigns of an assignment from the node.
   */
  private void doInternalUnassign(final Message<JsonObject> message) {
    final String id = message.body().getString("id");
    if (id == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No identifier found."));
    }
    else if (!assignments.containsKey(id)) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid assignment."));
    }
    else {
      AssignmentInfoInternal info = assignments.remove(id);
      if (info.type.equals(AssignmentInfoInternal.MODULE)) {
        container.undeployModule(info.id, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
            }
            else {
              message.reply(new JsonObject().putString("status", "ok"));
              message.reply(new JsonObject().putString("status", "ok"));
            }
          }
        });
      }
      else if (info.type.equals(AssignmentInfoInternal.VERTICLE)) {
        container.undeployVerticle(info.id, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
            }
            else {
              message.reply(new JsonObject().putString("status", "ok"));
            }
          }
        });
      }
    }
  }

  /**
   * Selects a node from a list of nodes based on the node with the least current
   * assignments.
   */
  private NodeReference pickNode(final Map<String, NodeReference> nodes) {
    String address = null;
    NodeReference pick = null;
    for (Map.Entry<String, NodeReference> entry : nodes.entrySet()) {
      if (pick == null || entry.getValue().info.assignments().size() < pick.info.assignments().size()) {
        address = entry.getKey();
        pick = entry.getValue();
      }
    }
    if (address != null) {
      nodes.remove(address);
    }
    return pick;
  }

  /**
   * Reassigns deployments from a failed node.
   */
  private void reassignNodeDeployments(final NodeReference node) {
    final Serializer serializer = SerializerFactory.getSerializer(NodeInfo.class);
    reassignNodeDeployments(node.info.assignments().iterator(), null, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          logger.warn(result.cause());
        }
        else {
          try {
            node.info = NodeInfo.Builder.newBuilder(node.info).setAssignments(new ArrayList<AssignmentInfo>()).build();
            context.submitCommand("updateNode", new JsonObject().putString("replica", node.replica)
                .putString("address", node.address).putObject("info", serializer.serializeToObject(node.info)), new Handler<AsyncResult<Boolean>>() {
              @Override
              public void handle(AsyncResult<Boolean> result) {
                if (result.failed()) {
                  logger.warn(result.cause());
                }
                else {
                  logger.info("Successfully redeployed " + node.address + " assignments.");
                }
              }
            });
          }
          catch (SerializationException e) {
            logger.warn(e);
          }
        }
      }
    });
  }

  /**
   * Reassigns deployments from a failed node.
   */
  private void reassignNodeDeployments(final Iterator<AssignmentInfo> iterator, final Throwable t,
      final Handler<AsyncResult<Void>> doneHandler) {
    if (iterator.hasNext()) {
      AssignmentInfo assignmentInfo = iterator.next();
      doDeployInstances(1, Arrays.asList(new InstanceInfo[] { assignmentInfo.instance() }), assignmentInfo.instance().deployment(),
          new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              reassignNodeDeployments(iterator, result.failed() ? (t != null ? t : result.cause()) : t, doneHandler);
            }
          });
    }
    else {
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
    }
  }

  /**
   * A reference to a remote node.
   */
  private class NodeReference implements Serializable {
    private final String address;
    private String replica;
    @JsonIgnore
    private long timerID;
    @JsonIgnore
    private Handler<String> timeoutHandler;
    @JsonProperty
    private NodeInfo info;

    private NodeReference() {
      address = null;
    }

    private NodeReference(String address, String replica) {
      this.address = address;
      this.replica = replica;
      info = NodeInfo.Builder.newBuilder().setAddress(address).build();
    }

    /**
     * Resets the node broadcast timer.
     */
    private NodeReference resetTimer() {
      cancelTimer();
      timerID = vertx.setTimer(5000, new Handler<Long>() {
        @Override
        public void handle(Long timerID) {
          NodeReference.this.timerID = 0;
          if (timeoutHandler != null) {
            timeoutHandler.handle(address);
          }
        }
      });
      return this;
    }

    /**
     * Cancels the node broadcast timer.
     */
    private NodeReference cancelTimer() {
      if (timerID > 0) {
        vertx.cancelTimer(timerID);
      }
      return this;
    }

    /**
     * Sets the broadcast timeout handler.
     */
    private NodeReference timeoutHandler(Handler<String> handler) {
      timeoutHandler = handler;
      return this;
    }
  }

  /**
   * Node assignment info.
   */
  private static class AssignmentInfoInternal implements Serializable {
    private static final String MODULE = "module";
    private static final String VERTICLE = "verticle";
    private final String id;
    private final String type;

    private AssignmentInfoInternal(String id, String type) {
      this.id = id;
      this.type = type;
    }
  }

}
