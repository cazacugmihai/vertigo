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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import net.kuujo.vertigo.cluster.ClusterException;
import net.kuujo.vertigo.cluster.ClusterManager;
import net.kuujo.vertigo.cluster.DeploymentInfo;
import net.kuujo.vertigo.cluster.Event;
import net.kuujo.vertigo.cluster.Event.Type;
import net.kuujo.vertigo.cluster.NodeInfo;

/**
 * A default cluster manager implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultClusterManager implements ClusterManager {
  private static final long DEFAULT_TIMEOUT = 30000;
  private final String address;
  private final Vertx vertx;
  private static final ObjectMapper mapper = new ObjectMapper();
  private final Map<String, Map<Handler<Event>, HandlerWrapper>> watchHandlers = new HashMap<>();

  private static class HandlerWrapper {
    private final String address;
    private final Handler<Message<JsonObject>> messageHandler;

    private HandlerWrapper(String address, Handler<Message<JsonObject>> messageHandler) {
      this.address = address;
      this.messageHandler = messageHandler;
    }
  }

  public DefaultClusterManager(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public ClusterManager set(String key, Object value) {
    return set(key, value, 0, null);
  }

  @Override
  public ClusterManager set(String key, Object value, long expire) {
    return set(key, value, expire, null);
  }

  @Override
  public ClusterManager set(String key, Object value, Handler<AsyncResult<Void>> doneHandler) {
    return set(key, value, 0, doneHandler);
  }

  @Override
  public ClusterManager set(String key, Object value, long expire, Handler<AsyncResult<Void>> doneHandler) {
    JsonObject message = new JsonObject().putString("action", "set").putString("key", key).putValue("vaule", value)
        .putNumber("expire", expire);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, createAsyncVoidHandler(doneHandler));
    return this;
  }

  @Override
  public <T> ClusterManager get(String key, Handler<AsyncResult<T>> resultHandler) {
    JsonObject message = new JsonObject().putString("action", "get").putString("key", key);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, createAsyncValueHandler(resultHandler));
    return this;
  }

  @Override
  public ClusterManager exists(String key, Handler<AsyncResult<Boolean>> resultHandler) {
    JsonObject message = new JsonObject().putString("action", "exists").putString("key", key);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, createAsyncValueHandler(resultHandler));
    return this;
  }

  @Override
  public ClusterManager keys(final Handler<AsyncResult<Collection<String>>> resultHandler) {
    vertx.eventBus().sendWithTimeout(address, new JsonObject().putString("action", "keys"), DEFAULT_TIMEOUT,
        new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            if (result.failed()) {
              new DefaultFutureResult<Collection<String>>(result.cause()).setHandler(resultHandler);
            }
            else {
              JsonObject body = result.result().body();
              if (body.getString("status").equals("error")) {
                new DefaultFutureResult<Collection<String>>(new ClusterException(body.getString("message"))).setHandler(resultHandler);
              }
              else {
                JsonArray jsonKeys = body.getArray("keys");
                if (jsonKeys != null) {
                  new DefaultFutureResult<Collection<String>>(Arrays.<String> asList((String[]) jsonKeys.toArray()))
                      .setHandler(resultHandler);
                }
              }
            }
          }
        });
    return this;
  }

  @Override
  public ClusterManager timeout(String key, long timeout) {
    return timeout(key, timeout, null);
  }

  @Override
  public ClusterManager timeout(String key, long timeout, Handler<AsyncResult<Void>> doneHandler) {
    JsonObject message = new JsonObject().putString("action", "timeout").putString("key", key).putNumber("timeout", timeout);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, createAsyncVoidHandler(doneHandler));
    return this;
  }

  @Override
  public ClusterManager reset(String key) {
    return reset(key, null);
  }

  @Override
  public ClusterManager reset(String key, Handler<AsyncResult<Void>> doneHandler) {
    JsonObject message = new JsonObject().putString("action", "reset").putString("key", key);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, createAsyncVoidHandler(doneHandler));
    return this;
  }

  @Override
  public ClusterManager watch(String key, Handler<Event> handler) {
    return watch(key, handler, null);
  }

  @Override
  public ClusterManager watch(String key, Type event, Handler<Event> handler) {
    return watch(key, event, handler, null);
  }

  @Override
  public ClusterManager watch(final String key, final Handler<Event> handler, final Handler<AsyncResult<Void>> doneHandler) {
    return watch(key, null, handler, doneHandler);
  }

  @Override
  public ClusterManager watch(final String key, final Type event, final Handler<Event> handler, final Handler<AsyncResult<Void>> doneHandler) {
    final String id = UUID.randomUUID().toString();
    final Handler<Message<JsonObject>> watchHandler = new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        try {
          handler.handle(mapper.readValue(message.body().encode(), Event.class));
        }
        catch (IOException e) {
        }
      }
    };

    final HandlerWrapper wrapper = new HandlerWrapper(id, watchHandler);

    vertx.eventBus().registerHandler(id, watchHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        }
        else {
          final Map<Handler<Event>, HandlerWrapper> handlers;
          if (watchHandlers.containsKey(key)) {
            handlers = watchHandlers.get(key);
          }
          else {
            handlers = new HashMap<>();
            watchHandlers.put(key, handlers);
          }
          handlers.put(handler, wrapper);
          JsonObject message = new JsonObject().putString("action", "watch").putString("key", key)
              .putString("event", event != null ? event.toString() : null).putString("address", id);
          vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
              if (result.failed()) {
                vertx.eventBus().unregisterHandler(id, watchHandler);
                handlers.remove(handler);
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              }
              else {
                JsonObject body = result.result().body();
                if (body.getString("status").equals("error")) {
                  vertx.eventBus().unregisterHandler(id, watchHandler);
                  handlers.remove(handler);
                  new DefaultFutureResult<Void>(new ClusterException(body.getString("message"))).setHandler(doneHandler);
                }
                else {
                  new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                }
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public ClusterManager unwatch(String key, Handler<Event> handler) {
    return unwatch(key, handler, null);
  }

  @Override
  public ClusterManager unwatch(String key, Type event, Handler<Event> handler) {
    return unwatch(key, event, handler, null);
  }

  @Override
  public ClusterManager unwatch(String key, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler) {
    return unwatch(key, null, handler, doneHandler);
  }

  @Override
  public ClusterManager unwatch(final String key, final Type event, final Handler<Event> handler,
      final Handler<AsyncResult<Void>> doneHandler) {
    final Map<Handler<Event>, HandlerWrapper> handlers = watchHandlers.get(key);
    if (!handlers.containsKey(handler)) {
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
      return this;
    }

    JsonObject message = new JsonObject().putString("action", "unwatch").putString("key", key)
        .putString("event", event != null ? event.toString() : null).putString("address", handlers.get(handler).address);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          HandlerWrapper wrapper = handlers.remove(handler);
          vertx.eventBus().unregisterHandler(wrapper.address, wrapper.messageHandler);
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        }
        else {
          HandlerWrapper wrapper = handlers.remove(handler);
          vertx.eventBus().unregisterHandler(wrapper.address, wrapper.messageHandler, doneHandler);
        }
      }
    });
    return this;
  }

  @Override
  public ClusterManager getNodeInfo(final Handler<AsyncResult<Collection<NodeInfo>>> resultHandler) {
    JsonObject message = new JsonObject().putString("action", "nodes");
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<Collection<NodeInfo>>(result.cause()).setHandler(resultHandler);
        }
        else {
          JsonObject body = result.result().body();
          if (body.getString("status").equals("error")) {
            new DefaultFutureResult<Collection<NodeInfo>>(new ClusterException(body.getString("message"))).setHandler(resultHandler);
          }
          else {
            JsonArray jsonNodes = body.getArray("result");
            if (jsonNodes != null) {
              List<NodeInfo> nodes = new ArrayList<>();
              for (Object jsonNode : jsonNodes) {
                try {
                  nodes.add(mapper.readValue(((JsonObject) jsonNode).encode(), NodeInfo.class));
                }
                catch (IOException e) {
                  continue;
                }
              }
              new DefaultFutureResult<Collection<NodeInfo>>(nodes).setHandler(resultHandler);
            }
          }
        }
      }
    });
    return this;
  }

  @Override
  public ClusterManager getNodeInfo(String address, final Handler<AsyncResult<NodeInfo>> resultHandler) {
    JsonObject message = new JsonObject().putString("action", "node").putString("address", address);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<NodeInfo>(result.cause()).setHandler(resultHandler);
        }
        else {
          JsonObject body = result.result().body();
          if (body.getString("status").equals("error")) {
            new DefaultFutureResult<NodeInfo>(new ClusterException(body.getString("message"))).setHandler(resultHandler);
          }
          else {
            try {
              new DefaultFutureResult<NodeInfo>(mapper.readValue(body.getObject("result").encode(), NodeInfo.class));
            }
            catch (IOException e) {
              new DefaultFutureResult<NodeInfo>(e).setHandler(resultHandler);
            }
          }
        }
      }
    });
    return this;
  }

  @Override
  public ClusterManager getDeploymentInfo(final Handler<AsyncResult<Collection<DeploymentInfo>>> resultHandler) {
    JsonObject message = new JsonObject().putString("action", "deployments");
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<Collection<DeploymentInfo>>(result.cause()).setHandler(resultHandler);
        }
        else {
          JsonObject body = result.result().body();
          if (body.getString("status").equals("error")) {
            new DefaultFutureResult<Collection<DeploymentInfo>>(new ClusterException(body.getString("message"))).setHandler(resultHandler);
          }
          else {
            JsonArray jsonNodes = body.getArray("result");
            if (jsonNodes != null) {
              List<DeploymentInfo> nodes = new ArrayList<>();
              for (Object jsonNode : jsonNodes) {
                try {
                  nodes.add(mapper.readValue(((JsonObject) jsonNode).encode(), DeploymentInfo.class));
                }
                catch (IOException e) {
                  continue;
                }
              }
              new DefaultFutureResult<Collection<DeploymentInfo>>(nodes).setHandler(resultHandler);
            }
          }
        }
      }
    });
    return this;
  }

  @Override
  public ClusterManager getDeploymentInfo(String deploymentID, final Handler<AsyncResult<DeploymentInfo>> resultHandler) {
    JsonObject message = new JsonObject().putString("action", "deployment").putString("id", deploymentID);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<DeploymentInfo>(result.cause()).setHandler(resultHandler);
        }
        else {
          JsonObject body = result.result().body();
          if (body.getString("status").equals("error")) {
            new DefaultFutureResult<DeploymentInfo>(new ClusterException(body.getString("message"))).setHandler(resultHandler);
          }
          else {
            try {
              new DefaultFutureResult<DeploymentInfo>(mapper.readValue(body.getObject("result").encode(), DeploymentInfo.class));
            }
            catch (IOException e) {
              new DefaultFutureResult<DeploymentInfo>(e).setHandler(resultHandler);
            }
          }
        }
      }
    });
    return this;
  }

  @Override
  public ClusterManager deployModule(String deploymentID, String moduleName, Handler<AsyncResult<String>> doneHandler) {
    return deployModule(deploymentID, moduleName, null, 1, doneHandler);
  }

  @Override
  public ClusterManager deployModule(String deploymentID, String moduleName, JsonObject config, Handler<AsyncResult<String>> doneHandler) {
    return deployModule(deploymentID, moduleName, config, 1, doneHandler);
  }

  @Override
  public ClusterManager deployModule(String deploymentID, String moduleName, int instances, Handler<AsyncResult<String>> doneHandler) {
    return deployModule(deploymentID, moduleName, null, instances, doneHandler);
  }

  @Override
  public ClusterManager deployModule(String deploymentID, String moduleName, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(null, "module", deploymentID, moduleName, config, instances, false, false, doneHandler);
  }

  @Override
  public ClusterManager deployModuleTo(String address, String deploymentID, String moduleName, Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(address, deploymentID, moduleName, null, 1, doneHandler);
  }

  @Override
  public ClusterManager deployModuleTo(String address, String deploymentID, String moduleName, JsonObject config,
      Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(address, deploymentID, moduleName, config, 1, doneHandler);
  }

  @Override
  public ClusterManager deployModuleTo(String address, String deploymentID, String moduleName, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(address, deploymentID, moduleName, null, instances, doneHandler);
  }

  @Override
  public ClusterManager deployModuleTo(String address, String deploymentID, String moduleName, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(new HashSet<String>(Arrays.asList(new String[] { address })), "module", deploymentID, moduleName, config, instances,
        false, false, doneHandler);
  }

  @Override
  public ClusterManager deployModuleTo(Set<String> nodes, String deploymentID, String moduleName, Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(nodes, deploymentID, moduleName, null, 1, doneHandler);
  }

  @Override
  public ClusterManager deployModuleTo(Set<String> nodes, String deploymentID, String moduleName, JsonObject config,
      Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(nodes, deploymentID, moduleName, config, 1, doneHandler);
  }

  @Override
  public ClusterManager deployModuleTo(Set<String> nodes, String deploymentID, String moduleName, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployModuleTo(nodes, deploymentID, moduleName, null, instances, doneHandler);
  }

  @Override
  public ClusterManager deployModuleTo(Set<String> nodes, String deploymentID, String moduleName, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(nodes, "module", deploymentID, moduleName, config, instances, false, false, doneHandler);
  }

  @Override
  public ClusterManager deployVerticle(String deploymentID, String main, Handler<AsyncResult<String>> doneHandler) {
    return deployVerticle(deploymentID, main, null, 1, doneHandler);
  }

  @Override
  public ClusterManager deployVerticle(String deploymentID, String main, JsonObject config, Handler<AsyncResult<String>> doneHandler) {
    return deployVerticle(deploymentID, main, config, 1, doneHandler);
  }

  @Override
  public ClusterManager deployVerticle(String deploymentID, String main, int instances, Handler<AsyncResult<String>> doneHandler) {
    return deployVerticle(deploymentID, main, null, instances, doneHandler);
  }

  @Override
  public ClusterManager deployVerticle(String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(null, "verticle", deploymentID, main, config, instances, false, false, doneHandler);
  }

  @Override
  public ClusterManager deployVerticleTo(String address, String deploymentID, String main, Handler<AsyncResult<String>> doneHandler) {
    return deployVerticleTo(address, deploymentID, main, null, 1, doneHandler);
  }

  @Override
  public ClusterManager deployVerticleTo(String address, String deploymentID, String main, JsonObject config,
      Handler<AsyncResult<String>> doneHandler) {
    return deployVerticleTo(address, deploymentID, main, config, 1, doneHandler);
  }

  @Override
  public ClusterManager deployVerticleTo(String address, String deploymentID, String main, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployVerticleTo(address, deploymentID, main, null, instances, doneHandler);
  }

  @Override
  public ClusterManager deployVerticleTo(String address, String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(new HashSet<String>(Arrays.asList(new String[] { address })), "verticle", deploymentID, main, config, instances, false,
        false, doneHandler);
  }

  @Override
  public ClusterManager deployVerticleTo(Set<String> nodes, String deploymentID, String main, Handler<AsyncResult<String>> doneHandler) {
    return deployVerticleTo(nodes, deploymentID, main, null, 1, doneHandler);
  }

  @Override
  public ClusterManager deployVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config,
      Handler<AsyncResult<String>> doneHandler) {
    return deployVerticleTo(nodes, deploymentID, main, config, 1, doneHandler);
  }

  @Override
  public ClusterManager deployVerticleTo(Set<String> nodes, String deploymentID, String main, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployVerticleTo(nodes, deploymentID, main, null, instances, doneHandler);
  }

  @Override
  public ClusterManager deployVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(nodes, "verticle", deploymentID, main, config, instances, false, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticle(String deploymentID, String main, Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticle(deploymentID, main, null, 1, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticle(String deploymentID, String main, JsonObject config, Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticle(deploymentID, main, config, 1, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticle(String deploymentID, String main, int instances, Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticle(deploymentID, main, null, instances, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticle(String deploymentID, String main, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticle(deploymentID, main, null, 1, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticle(String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticle(deploymentID, main, config, instances, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticle(String deploymentID, String main, JsonObject config, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticle(deploymentID, main, config, 1, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticle(String deploymentID, String main, int instances, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticle(deploymentID, main, null, instances, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticle(String deploymentID, String main, JsonObject config, int instances, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(null, "verticle", deploymentID, main, config, instances, true, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(address, deploymentID, main, null, 1, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, JsonObject config,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(address, deploymentID, main, config, 1, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(address, deploymentID, main, null, instances, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(address, deploymentID, main, null, 1, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(address, deploymentID, main, config, instances, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, JsonObject config, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(address, deploymentID, main, config, 1, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, int instances, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(address, deploymentID, main, null, instances, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, JsonObject config, int instances,
      boolean multiThreaded, Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(new HashSet<String>(Arrays.asList(new String[] { address })), "verticle", deploymentID, main, config, instances, true,
        multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(nodes, deploymentID, main, null, 1, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(nodes, deploymentID, main, config, 1, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(nodes, deploymentID, main, null, instances, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(nodes, deploymentID, main, null, 1, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(nodes, deploymentID, main, config, instances, false, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config,
      boolean multiThreaded, Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(nodes, deploymentID, main, config, 1, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, int instances, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleTo(nodes, deploymentID, main, null, instances, multiThreaded, doneHandler);
  }

  @Override
  public ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config, int instances,
      boolean multiThreaded, Handler<AsyncResult<String>> doneHandler) {
    return doDeploy(nodes, "verticle", deploymentID, main, config, instances, true, multiThreaded, doneHandler);
  }

  private ClusterManager doDeploy(Set<String> targets, String type, String deploymentID, String main, JsonObject config, int instances,
      boolean worker, boolean multiThreaded, Handler<AsyncResult<String>> doneHandler) {
    JsonObject message = new JsonObject().putString("action", "deploy")
        .putArray("targets", targets != null ? new JsonArray(targets.toArray(new String[targets.size()])) : new JsonArray())
        .putString("id", deploymentID).putString("type", type).putString(type.equals("module") ? "module" : "main", main)
        .putObject("config", config).putNumber("instances", instances).putBoolean("worker", worker)
        .putBoolean("multi-threaded", multiThreaded);
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, createAsyncValueHandler(doneHandler));
    return this;
  }

  @Override
  public ClusterManager undeployModule(String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    return doUndeploy(null, "module", deploymentID, doneHandler);
  }

  @Override
  public ClusterManager undeployModuleFrom(String address, String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    return doUndeploy(new HashSet<String>(Arrays.asList(new String[] { address })), "module", deploymentID, doneHandler);
  }

  @Override
  public ClusterManager undeployModuleFrom(Set<String> nodes, String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    return doUndeploy(nodes, "module", deploymentID, doneHandler);
  }

  @Override
  public ClusterManager undeployVerticle(String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    return doUndeploy(null, "verticle", deploymentID, doneHandler);
  }

  @Override
  public ClusterManager undeployVerticleFrom(String address, String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    return doUndeploy(new HashSet<String>(Arrays.asList(new String[] { address })), "verticle", deploymentID, doneHandler);
  }

  @Override
  public ClusterManager undeployVerticleFrom(Set<String> nodes, String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    return doUndeploy(nodes, "verticle", deploymentID, doneHandler);
  }

  private ClusterManager doUndeploy(Set<String> targets, String type, String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    JsonObject message = new JsonObject().putString("action", "undeploy").putString("type", type).putString("id", deploymentID)
        .putArray("targets", targets != null ? new JsonArray(targets.toArray(new String[targets.size()])) : new JsonArray());
    vertx.eventBus().sendWithTimeout(address, message, DEFAULT_TIMEOUT, createAsyncVoidHandler(doneHandler));
    return this;
  }

  private Handler<AsyncResult<Message<JsonObject>>> createAsyncVoidHandler(final Handler<AsyncResult<Void>> doneHandler) {
    return new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        }
        else {
          JsonObject body = result.result().body();
          if (body.getString("status").equals("error")) {
            new DefaultFutureResult<Void>(new ClusterException(body.getString("message"))).setHandler(doneHandler);
          }
          else {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          }
        }
      }
    };
  }

  private <T> Handler<AsyncResult<Message<JsonObject>>> createAsyncValueHandler(final Handler<AsyncResult<T>> doneHandler) {
    return new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      @SuppressWarnings("unchecked")
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<T>(result.cause()).setHandler(doneHandler);
        }
        else {
          JsonObject body = result.result().body();
          if (body.getString("status").equals("error")) {
            new DefaultFutureResult<T>(new ClusterException(body.getString("message"))).setHandler(doneHandler);
          }
          else {
            new DefaultFutureResult<T>((T) body.getValue("result")).setHandler(doneHandler);
          }
        }
      }
    };
  }

}
