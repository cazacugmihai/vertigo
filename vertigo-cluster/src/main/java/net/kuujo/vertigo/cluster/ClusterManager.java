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

import java.util.Collection;
import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * A Vertigo cluster manager.
 * 
 * @author Jordan Halterman
 */
public interface ClusterManager {

  /**
   * Returns the address of the cluster.
   * 
   * @return The address of the cluster.
   */
  String address();

  /**
   * Sets a key in the cluster.
   * 
   * @param key The key to set.
   * @param value The value to set.
   * @return The cluster manager.
   */
  ClusterManager set(String key, Object value);

  /**
   * Sets a key in the cluster.
   * 
   * @param key The key to set.
   * @param value The value to set.
   * @param expire A key expiration in milliseconds.
   * @return The cluster manager.
   */
  ClusterManager set(String key, Object value, long expire);

  /**
   * Sets a key in the cluster.
   * 
   * @param key The key to set.
   * @param value The value to set.
   * @param doneHandler An asynchronous handler to be called once complete.
   * @return The cluster manager.
   */
  ClusterManager set(String key, Object value, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Sets a key in the cluster.
   * 
   * @param key The key to set.
   * @param value The value to set.
   * @param expire A key expiration in milliseconds.
   * @param doneHandler An asynchronous handler to be called once complete.
   * @return The cluster manager.
   */
  ClusterManager set(String key, Object value, long expire, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Gets a key from the cluster.
   * 
   * @param key The key to get.
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster manager.
   */
  <T> ClusterManager get(String key, Handler<AsyncResult<T>> resultHandler);

  /**
   * Checks if a key exists in the cluster.
   * 
   * @param key The key to check.
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster manager.
   */
  ClusterManager exists(String key, Handler<AsyncResult<Boolean>> resultHandler);

  /**
   * Gets a collection of keys in the cluster.
   * 
   * @param resultHandler An asynchronous handler to be called with a collection of
   *          available keys in the cluster.
   * @return The cluster manager.
   */
  ClusterManager keys(Handler<AsyncResult<Collection<String>>> resultHandler);

  /**
   * Sets a timeout in the cluster.
   * 
   * @param key The key to timeout.
   * @param timeout The timeout in milliseconds.
   * @return The cluster manager.
   */
  ClusterManager timeout(String key, long timeout);

  /**
   * Sets a timeout in the cluster.
   * 
   * @param key The key to timeout.
   * @param timeout The timeout in milliseconds.
   * @param doneHandler An asycnhronous handler to be called once the timeout is set.
   * @return The cluster manager.
   */
  ClusterManager timeout(String key, long timeout, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Resets a key in the cluster.
   * 
   * @param key The key to reset.
   * @return The cluster manager.
   */
  ClusterManager reset(String key);

  /**
   * Resets a key in the cluster.
   * 
   * @param key The key to reset.
   * @param doneHandler An asynchronous handler to be called once the timeout has been
   *          reset.
   * @return The cluster manager.
   */
  ClusterManager reset(String key, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Watches a key in the cluster.
   * 
   * @param key The key to watch.
   * @param handler A handler to be called when an event occurs.
   * @return The cluster manager.
   */
  ClusterManager watch(String key, Handler<Event> handler);

  /**
   * Watches a key in the cluster for a specific event.
   * 
   * @param key The key to watch.
   * @param event The event to watch.
   * @param handler A handler to be called when the event occurs.
   * @return The cluster manager.
   */
  ClusterManager watch(String key, Event.Type event, Handler<Event> handler);

  /**
   * Watches a key in the cluster.
   * 
   * @param key The key to watch.
   * @param handler A handler to be called when an event occurs.
   * @param doneHandler An asynchronous handler to be called once the key is being
   *          watched.
   * @return The cluster manager.
   */
  ClusterManager watch(String key, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Watches a key in the cluster for a specific event.
   * 
   * @param key The key to watch.
   * @param event The event to watch.
   * @param handler A handler to be called when the event occurs.
   * @param doneHandler An asynchronous handler to be called once the key is being
   *          watched.
   * @return The cluster manager.
   */
  ClusterManager watch(String key, Event.Type event, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Unwatches a key in the cluster.
   * 
   * @param key The key to unwatch.
   * @param handler The handler to unwatch.
   * @return The cluster manager.
   */
  ClusterManager unwatch(String key, Handler<Event> handler);

  /**
   * Unwatches a key in the cluster for a specific event.
   * 
   * @param key The key to unwatch.
   * @param event The event to unwatch.
   * @param handler The handler to unwatch.
   * @return The cluster manager.
   */
  ClusterManager unwatch(String key, Event.Type event, Handler<Event> handler);

  /**
   * Unwatches a key in the cluster.
   * 
   * @param key The key to unwatch.
   * @param handler The handler to unwatch.
   * @param doneHandler An asynchronous handler to be called once the key has been
   *          unwatched.
   * @return The cluster manager.
   */
  ClusterManager unwatch(String key, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Unwatches a key in the cluster for a specific event.
   * 
   * @param key The key to unwatch.
   * @param event The event to unwatch.
   * @param handler The handler to unwatch.
   * @param doneHandler An asynchronous handler to be called once the key has been
   *          unwatched.
   * @return The cluster manager.
   */
  ClusterManager unwatch(String key, Event.Type event, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Fetches a list of all nodes currently running in the cluster.
   * 
   * @param resultHandler An asynchronous handler to be called once the reply is received.
   * @return The cluster manager.
   */
  ClusterManager getNodeInfo(Handler<AsyncResult<Collection<NodeInfo>>> resultHandler);

  /**
   * Fetches node info for a specific node in the cluster.
   * 
   * @param address The address of the node for which to get info.
   * @param resultHandler An asynchronous handler to be called once the reply is received.
   * @return The cluster manager.
   */
  ClusterManager getNodeInfo(String address, Handler<AsyncResult<NodeInfo>> resultHandler);

  /**
   * Fetches a list of all deployments currently running in the cluster.
   * 
   * @param resultHandler An asynchronous handler to be called once the reply is received.
   * @return The cluster manager.
   */
  ClusterManager getDeploymentInfo(Handler<AsyncResult<Collection<DeploymentInfo>>> resultHandler);

  /**
   * Fetches deployment info for a specific deployment in the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param resultHandler An asynchronous handler to be called once the reply is received.
   * @return The cluster manager.
   */
  ClusterManager getDeploymentInfo(String deploymentID, Handler<AsyncResult<DeploymentInfo>> resultHandler);

  /**
   * Deploys a module to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModule(String deploymentID, String moduleName, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModule(String deploymentID, String moduleName, JsonObject config, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModule(String deploymentID, String moduleName, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModule(String deploymentID, String moduleName, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the module.
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModuleTo(String address, String deploymentID, String moduleName, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the module.
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModuleTo(String address, String deploymentID, String moduleName, JsonObject config,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the module.
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModuleTo(String address, String deploymentID, String moduleName, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the module.
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModuleTo(String address, String deploymentID, String moduleName, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to a a group of nodes in the cluster.
   * 
   * @param nodes A set of address of nodes in the group.
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModuleTo(Set<String> nodes, String deploymentID, String moduleName, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to a a group of nodes in the cluster.
   * 
   * @param nodes A set of address of nodes in the group.
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModuleTo(Set<String> nodes, String deploymentID, String moduleName, JsonObject config,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to a a group of nodes in the cluster.
   * 
   * @param nodes A set of address of nodes in the group.
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModuleTo(Set<String> nodes, String deploymentID, String moduleName, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to a a group of nodes in the cluster.
   * 
   * @param nodes A set of address of nodes in the group.
   * @param deploymentID The deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployModuleTo(Set<String> nodes, String deploymentID, String moduleName, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticle(String deploymentID, String main, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticle(String deploymentID, String main, JsonObject config, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticle(String deploymentID, String main, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticle(String deploymentID, String main, JsonObject config, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticleTo(String address, String deploymentID, String main, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticleTo(String address, String deploymentID, String main, JsonObject config,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticleTo(String address, String deploymentID, String main, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticleTo(String address, String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticleTo(Set<String> nodes, String deploymentID, String main, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticleTo(Set<String> nodes, String deploymentID, String main, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticle(String deploymentID, String main, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticle(String deploymentID, String main, JsonObject config, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticle(String deploymentID, String main, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticle(String deploymentID, String main, boolean multiThreaded, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticle(String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticle(String deploymentID, String main, JsonObject config, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticle(String deploymentID, String main, int instances, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticle(String deploymentID, String main, JsonObject config, int instances, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, JsonObject config,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, JsonObject config, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, int instances, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a specific node in the cluster.
   * 
   * @param address The address of the node to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(String address, String deploymentID, String main, JsonObject config, int instances,
      boolean multiThreaded, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config, int instances,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, int instances, boolean multiThreaded,
      Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle to a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes to which to deploy the verticle.
   * @param deploymentID The deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the deployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager deployWorkerVerticleTo(Set<String> nodes, String deploymentID, String main, JsonObject config, int instances,
      boolean multiThreaded, Handler<AsyncResult<String>> doneHandler);

  /**
   * Undeploys a module from the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param doneHandler An asynchronous handler to be called once the undeployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager undeployModule(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Undeploys a module from a specific node in the cluster.
   * 
   * @param address The address of the node from which to undeploy the module.
   * @param deploymentID The deployment ID.
   * @param doneHandler An asynchronous handler to be called once the undeployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager undeployModuleFrom(String address, String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Undeploys a module from a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes from which to undeploy the module.
   * @param deploymentID The deployment ID.
   * @param doneHandler An asynchronous handler to be called once the undeployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager undeployModuleFrom(Set<String> nodes, String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Undeploys a verticle from the cluster.
   * 
   * @param deploymentID The deployment ID.
   * @param doneHandler An asynchronous handler to be called once the undeployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager undeployVerticle(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Undeploys a verticle from a specific node in the cluster.
   * 
   * @param address The address of the node from which to undeploy the verticle.
   * @param deploymentID The deployment ID.
   * @param doneHandler An asynchronous handler to be called once the undeployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager undeployVerticleFrom(String address, String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Undeploys a verticle from a group of nodes in the cluster.
   * 
   * @param nodes The addresses of the nodes from which to undeploy the verticle.
   * @param deploymentID The deployment ID.
   * @param doneHandler An asynchronous handler to be called once the undeployment is
   *          complete or fails.
   * @return The cluster manager.
   */
  ClusterManager undeployVerticleFrom(Set<String> nodes, String deploymentID, Handler<AsyncResult<Void>> doneHandler);

}
