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
package net.kuujo.vertigo.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.kuujo.copycat.ClusterConfig;
import net.kuujo.copycat.CopyCat;
import net.kuujo.copycat.Replica;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.annotations.Command;
import net.kuujo.copycat.annotations.Command.Argument;
import net.kuujo.copycat.annotations.Stateful;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A Vertigo node.
 * 
 * @author Jordan Halterman
 */
public class VertigoNode extends BusModBase implements StateMachine {
  private static final ObjectMapper mapper = new ObjectMapper();
  private String clusterAddress;
  private String nodeAddress;
  private String internalAddress;
  private ClusterConfig config = new ClusterConfig();
  private Replica replica;
  private boolean started;
  private Future<Void> startFuture;
  private final Map<String, Set<Watcher>> watchers = new HashMap<>();
  @Stateful
  private final Map<String, NodeReference> nodes = new HashMap<>();
  @Stateful
  private final Map<String, AssignmentInfoInternal> assignments = new HashMap<>();
  @Stateful
  private final Map<String, Value> data = new HashMap<>();

  // Handles messages sent to the cluster.
  private final Handler<Message<JsonObject>> clusterHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = getMandatoryString("action", message);
      if (action != null) {
        switch (action) {
          case "broadcast":
            doClusterBroadcast(message);
            break;
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
            sendError(message, "Invalid action " + action);
            break;
        }
      }
    }
  };

  // Handles message sent directly to the node.
  private final Handler<Message<JsonObject>> nodeHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = getMandatoryString("action", message);
      if (action != null) {
        switch (action) {
          case "deploy":
            doNodeDeploy(message);
            break;
          case "undeploy":
            doNodeUndeploy(message);
            break;
          default:
            sendError(message, "Invalid action " + action);
            break;
        }
      }
    }
  };

  // Handles messages sent between nodes.
  private final Handler<Message<JsonObject>> internalHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = getMandatoryString("action", message);
      if (action != null) {
        switch (action) {
          case "deploy":
            doInternalDeploy(message);
            break;
          case "assign":
            doInternalAssign(message);
            break;
          case "undeploy":
            doInternalUndeploy(message);
            break;
          case "unassign":
            doInternalUnassign(message);
            break;
          default:
            sendError(message, "Invalid action " + action);
            break;
        }
      }
    }
  };

  // A periodic broadcast timer. Broadcasting is used to notify other nodes
  // within the cluster of this node's existence.
  private final Handler<Long> broadcastTimer = new Handler<Long>() {
    @Override
    public void handle(Long timerID) {
      eb.publish(
          clusterAddress,
          new JsonObject().putString("action", "broadcast").putString("address", nodeAddress).putString("id", internalAddress)
              .putString("replica", replica.address()));
    }
  };

  // A node broadcast timeout handler. This handler is triggered when a node
  // has not broadcasted its presence for some period of time.
  private final Handler<String> timeoutHandler = new Handler<String>() {
    @Override
    public void handle(String address) {
      if (replica.isLeader() && nodes.containsKey(address)) {
        final NodeReference node = nodes.get(address);
        replica.submitCommand("removeNode", new JsonObject().putString("address", address), new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            reassignNodeDeployments(node);
          }
        });
      }
    }
  };

  /**
   * A state machine command for adding a node to the cluster.
   * 
   * @param address The node address.
   * @param id The node ID.
   * @param replica The node's replica address.
   */
  @Command(type = Command.Type.WRITE)
  public void addNode(@Argument("address") String address, @Argument("id") String id, @Argument("replica") String replica) {
    NodeReference node;
    if (!nodes.containsKey(address)) {
      node = new NodeReference(address, id, replica).timeoutHandler(timeoutHandler);
      nodes.put(address, node);
    }
    else {
      node = nodes.get(address);
    }

    node.id = id;
    node.replica = replica;

    if (!started) {
      String leader = VertigoNode.this.replica.getCurrentLeader();
      if (leader != null && leader.equals(replica)) {
        started = true;
        startFuture.setResult((Void) null);
      }
    }
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
    node.id = id;
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
    private Object value;
    private long expire;
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
  private static class Watcher {
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
    data.put(key, new Value(value, expire));
    if (replica.isLeader()) {
      triggerEvent("set", key, value);
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

        if (replica.isLeader()) {
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

      if (replica.isLeader()) {
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
   * A simple key timeout command.
   * 
   * @param key The key to timeout.
   * @param timeout The key timeout.
   */
  @Command(type = Command.Type.WRITE)
  public void timeout(final @Argument("key") String key, @Argument(value = "timeout", required = false) Long timeout) {
    if (data.containsKey(key)) {
      Value oldValue = data.get(key);
      if (oldValue.timer > 0) {
        vertx.cancelTimer(oldValue.timer);
      }
      if (timeout == null) {
        timeout = oldValue.expire;
      }
    }

    if (timeout == null) {
      throw new IllegalArgumentException("No timeout specified.");
    }

    final Value value = new Value(timeout, timeout);
    value.timer = vertx.setTimer(timeout, new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        value.timer = 0;

        if (data.containsKey(key)) {
          data.remove(key);
        }

        if (replica.isLeader()) {
          triggerEvent("timeout", key, value.expire);
        }
      }
    });

    data.put(key, value);

    if (replica.isLeader()) {
      triggerEvent("set", key, timeout);
    }
  }

  /**
   * A simple timeout cancel command.
   * 
   * @param key The key to cancel.
   * @return Indicates whether the key was cancelled.
   */
  @Command(type = Command.Type.WRITE)
  public boolean cancel(@Argument("key") String key) {
    if (data.containsKey(key) && data.get(key).timer > 0) {
      Value value = data.remove(key);
      vertx.cancelTimer(value.timer);
      value.timer = 0;
      return true;
    }
    return false;
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

  @Override
  public void start(final Future<Void> startResult) {
    super.start();
    startFuture = startResult;
    clusterAddress = getMandatoryStringConfig("cluster");
    nodeAddress = getMandatoryStringConfig("address");
    CopyCat copycat = new CopyCat(this);
    replica = copycat.createReplica(String.format("%s.replica", nodeAddress), this, config);
    internalAddress = String.format("%s.internal", replica.address());
    vertx.setPeriodic(1000, broadcastTimer);

    // Start the replica first. We need the replica to start and determine a
    // cluster leader before registering handlers on the event bus.
    replica.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          startResult.setFailure(result.cause());
        }
        else {
          // Register an internal (node-to-node) message handler.
          eb.registerHandler(internalAddress, internalHandler, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                startResult.setFailure(result.cause());
              }
              else {
                // Register a public node message handler.
                eb.registerHandler(nodeAddress, nodeHandler, new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    if (result.failed()) {
                      startResult.setFailure(result.cause());
                    }
                    else {
                      // Register a public cluster message handler.
                      eb.registerHandler(clusterAddress, clusterHandler, new Handler<AsyncResult<Void>>() {
                        @Override
                        public void handle(AsyncResult<Void> result) {
                          if (result.failed()) {
                            startResult.setFailure(result.cause());
                          }
                          else {
                            addNode(nodeAddress, internalAddress, replica.address());
                            startResult.setResult((Void) null);
                          }
                        }
                      });
                    }
                  }
                });
              }
            }
          });
        }
      }
    });
  }

  @Override
  public void stop() {
    replica.stop();
  }

  /**
   * Finds the internal address of the current cluster leader.
   */
  private String findLeader() {
    String leader = replica.getCurrentLeader();
    return leader != null ? String.format("%s.internal", leader) : null;
  }

  /**
   * Forwards a message to the cluster leader.
   */
  private void forwardToLeader(final JsonObject body, final Message<JsonObject> message) {
    final String leader = findLeader();
    if (leader == null) {
      sendError(message, "Internal error. No leader found.");
    }
    else {
      eb.sendWithTimeout(leader, body, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
        @Override
        public void handle(AsyncResult<Message<JsonObject>> result) {
          if (result.failed()) {
            sendError(message, result.cause().getMessage());
          }
          else if (result.result().body().getString("status").equals("error")) {
            sendError(message, result.result().body().getString("message"));
          }
          else {
            sendOK(message, result.result().body());
          }
        }
      });
    }
  }

  /**
   * Forwards a message to the cluster leader.
   */
  private void forwardToLeader(final Message<JsonObject> message) {
    forwardToLeader(message.body(), message);
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
    if (this.replica.isLeader()) {
      final String address = message.body().getString("address");
      final String id = message.body().getString("id");
      final String replica = message.body().getString("replica");
      if (!nodes.containsKey(address)) {
        if (!config.containsMember(replica)) {
          config.addMember(replica);
        }
        this.replica.submitCommand("addNode", message.body(), new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (nodes.containsKey(address)) {
              nodes.get(address).resetTimer();
            }
          }
        });
      }
      else if (!nodes.get(address).id.equals(id) || !nodes.get(address).replica.equals(replica)) {
        this.replica.submitCommand("updateNode", message.body(), new Handler<AsyncResult<Boolean>>() {
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
    replica.submitCommand("set", message.body(), new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message);
        }
      }
    });
  }

  /**
   * Called when a get command is received.
   */
  private void doClusterGet(final Message<JsonObject> message) {
    replica.submitCommand("get", message.body(), new Handler<AsyncResult<Object>>() {
      @Override
      public void handle(AsyncResult<Object> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else if (result.result() == null) {
          sendOK(message);
        }
        else {
          sendOK(message, new JsonObject().putValue("result", result.result()));
        }
      }
    });
  }

  /**
   * Called when a delete command is received.
   */
  private void doClusterDelete(final Message<JsonObject> message) {
    replica.submitCommand("delete", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message, new JsonObject().putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Called when a keys command is received.
   */
  private void doClusterKeys(final Message<JsonObject> message) {
    replica.submitCommand("keys", message.body(), new Handler<AsyncResult<List<String>>>() {
      @Override
      public void handle(AsyncResult<List<String>> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message, new JsonObject().putArray("result", new JsonArray(result.result().toArray(new Object[result.result().size()]))));
        }
      }
    });
  }

  /**
   * Called when a exists command is received.
   */
  private void doClusterExists(final Message<JsonObject> message) {
    replica.submitCommand("exists", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message, new JsonObject().putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Sets a timeout.
   */
  private void doClusterTimeout(final Message<JsonObject> message) {
    replica.submitCommand("timeout", message.body(), new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message);
        }
      }
    });
  }

  /**
   * Cancels a timer.
   */
  private void doClusterCancel(final Message<JsonObject> message) {
    replica.submitCommand("cancel", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message, new JsonObject().putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Watches a key.
   */
  private void doClusterWatch(final Message<JsonObject> message) {
    replica.submitCommand("watch", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message, new JsonObject().putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Unwatches a key.
   */
  private void doClusterUnwatch(final Message<JsonObject> message) {
    replica.submitCommand("unwatch", message.body(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message, new JsonObject().putBoolean("result", result.result()));
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
    eb.send(address, new JsonObject().putString("type", event).putString("key", key).putValue("value", value));
  }

  /**
   * Called when the cluster receives a deploy message. The cluster leader is the only
   * node that actually handles deployments, so the message is simply forwarded on to the
   * leader.
   */
  private void doClusterDeploy(final Message<JsonObject> message) {
    forwardToLeader(message);
  }

  /**
   * Called when the cluster receives an undeploy message. The cluster leader is the only
   * node that actually handles deployments, so the message is simply forwarded on to the
   * leader.
   */
  private void doClusterUndeploy(final Message<JsonObject> message) {
    forwardToLeader(message);
  }

  /**
   * Called when the node receives a deploy message. The cluster leader is the only node
   * that actually handles deployments, so the message is simply forwarded on to the
   * leader.
   */
  private void doNodeDeploy(final Message<JsonObject> message) {
    forwardToLeader(message.body().putString("target", nodeAddress), message);
  }

  /**
   * Called when the node receives an undeploy message. The cluster leader is the only
   * node that actually handles deployments, so the message is simply forwarded on to the
   * leader.
   */
  private void doNodeUndeploy(final Message<JsonObject> message) {
    forwardToLeader(message.body().putString("target", nodeAddress), message);
  }

  /**
   * Called when the node receives an internal deploy message. Internal deploy messages
   * should only ever be sent to the cluster leader. Thus, if this node is not the leader
   * then simply return an error.
   */
  private void doInternalDeploy(final Message<JsonObject> message) {
    if (!replica.isLeader()) {
      sendError(message, "Internal error. Not the leader");
      return;
    }

    DeploymentInfo deploymentInfo;
    try {
      deploymentInfo = mapper.readValue(message.body().encode(), DeploymentInfo.class);
    }
    catch (Exception e) {
      sendError(message, e.getMessage());
      return;
    }

    final String id = deploymentInfo.id();
    doDeployInstances(deploymentInfo, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message, new JsonObject().putString("id", id));
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
        try {
          eb.sendWithTimeout(node.id, new JsonObject(mapper.writeValueAsString(assignment)).putString("action", "assign"), 15000,
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
                    try {
                      replica.submitCommand("updateNode", new JsonObject().putString("replica", node.replica).putString("id", node.id)
                          .putString("address", node.address).putObject("info", new JsonObject(mapper.writeValueAsString(node))), new Handler<AsyncResult<Boolean>>() {
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
                    catch (JsonProcessingException e) {
                      new DefaultFutureResult<Void>(e).setHandler(doneHandler);
                    }
                  }
                }
              });
        }
        catch (JsonProcessingException e) {
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
    try {
      info = mapper.readValue(message.body().encode(), AssignmentInfo.class);
    }
    catch (IOException e) {
      sendError(message, e.getMessage());
      return;
    }

    final String id = info.instance().id();
    if (info.instance().deployment().type().equals(DeploymentInfo.Type.MODULE)) {
      ModuleDeploymentInfo deployment = (ModuleDeploymentInfo) info.instance().deployment();
      container.deployModule(deployment.module(), deployment.config(), 1, new Handler<AsyncResult<String>>() {
        @Override
        public void handle(AsyncResult<String> result) {
          if (result.failed()) {
            sendError(message, result.cause().getMessage());
          }
          else {
            assignments.put(id, new AssignmentInfoInternal(result.result(), AssignmentInfoInternal.MODULE));
            sendOK(message, new JsonObject().putString("id", id));
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
                  sendError(message, result.cause().getMessage());
                }
                else {
                  assignments.put(id, new AssignmentInfoInternal(result.result(), AssignmentInfoInternal.VERTICLE));
                  sendOK(message, new JsonObject().putString("id", id));
                }
              }
            });
      }
      else {
        container.deployVerticle(deployment.main(), deployment.config(), new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            if (result.failed()) {
              sendError(message, result.cause().getMessage());
            }
            else {
              assignments.put(id, new AssignmentInfoInternal(result.result(), AssignmentInfoInternal.VERTICLE));
              sendOK(message, new JsonObject().putString("id", id));
            }
          }
        });
      }
    }
    else {
      sendError(message, "Invalid deployment type.");
    }
  }

  /**
   * Handles an internal undeploy message.
   */
  private void doInternalUndeploy(final Message<JsonObject> message) {
    if (!replica.isLeader()) {
      sendError(message, "Internal error. Not the leader");
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
          sendError(message, result.cause().getMessage());
        }
        else {
          sendOK(message);
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
      eb.sendWithTimeout(node.id, new JsonObject().putString("action", "unassign").putString("id", assignment.instance().id()), 15000,
          new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(final AsyncResult<Message<JsonObject>> result) {
              NodeInfo nodeInfo = NodeInfo.Builder.newBuilder(node.info).removeAssignment(assignment).build();
              try {
                replica.submitCommand("updateNode",
                    new JsonObject().putString("id", node.id).putString("address", node.address).putString("replica", node.replica)
                        .putString("action", "unassign").putObject("info", new JsonObject(mapper.writeValueAsString(nodeInfo))),
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
              catch (JsonProcessingException e) {
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
    final String id = getMandatoryString("id", message);
    if (id == null) {
      sendError(message, "No identifier found.");
    }
    else if (!assignments.containsKey(id)) {
      sendError(message, "Invalid assignment.");
    }
    else {
      AssignmentInfoInternal info = assignments.remove(id);
      if (info.type.equals(AssignmentInfoInternal.MODULE)) {
        container.undeployModule(info.id, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              sendError(message, result.cause().getMessage());
            }
            else {
              sendOK(message);
            }
          }
        });
      }
      else if (info.type.equals(AssignmentInfoInternal.VERTICLE)) {
        container.undeployVerticle(info.id, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              sendError(message, result.cause().getMessage());
            }
            else {
              sendOK(message);
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
    reassignNodeDeployments(node.info.assignments().iterator(), null, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          container.logger().warn(result.cause());
        }
        else {
          container.logger().info("Successfully redeployed " + node.address + " assignments.");
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
  private class NodeReference {
    @JsonProperty
    private final String address;
    @JsonProperty
    private String id;
    @JsonProperty
    private String replica;
    @JsonIgnore
    private long timerID;
    private Handler<String> timeoutHandler;
    private NodeInfo info;

    private NodeReference(String address, String id, String replica) {
      this.address = address;
      this.id = id;
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
  private static class AssignmentInfoInternal {
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
