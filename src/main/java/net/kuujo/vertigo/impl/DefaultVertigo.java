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
package net.kuujo.vertigo.impl;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.Verticle;

import net.kuujo.vertigo.Vertigo;
import net.kuujo.vertigo.cluster.Cluster;
import net.kuujo.vertigo.cluster.LocalCluster;
import net.kuujo.vertigo.cluster.RemoteCluster;
import net.kuujo.vertigo.component.Component;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.context.NetworkContext;
import net.kuujo.vertigo.network.Network;

/**
 * Primary Vert.igo API.
 *
 * This is the primary API for creating Vertigo objects within component
 * implementations. This should be used to instantiate any feeders, workers, or
 * executors that are used by the component implementation.
 *
 * @author Jordan Halterman
 */
public final class DefaultVertigo implements Vertigo {
  private final Vertx vertx;
  private final Container container;
  private final Component<?> component;

  public DefaultVertigo(Verticle verticle) {
    this(verticle.getVertx(), verticle.getContainer());
  }

  public DefaultVertigo(Vertx vertx, Container container) {
    this.vertx = vertx;
    this.container = container;
    this.component = null;
  }

  @Override
  public boolean isComponent() {
    return component != null;
  }

  @Override
  public Component<?> component() {
    return component;
  }

  @Override
  public JsonObject config() {
    return container.config();
  }

  @Override
  public InstanceContext<?> context() {
    return isComponent() ? component.context() : null;
  }

  @Override
  public Network createNetwork(String address) {
    return new Network(address);
  }

  @Override
  public Vertigo deployLocalNetwork(Network network) {
    Cluster cluster = new LocalCluster(vertx, container);
    cluster.deployNetwork(network);
    return this;
  }

  @Override
  public Vertigo deployLocalNetwork(Network network, Handler<AsyncResult<NetworkContext>> doneHandler) {
    Cluster cluster = new LocalCluster(vertx, container);
    cluster.deployNetwork(network, doneHandler);
    return this;
  }

  @Override
  public Vertigo shutdownLocalNetwork(NetworkContext context) {
    Cluster cluster = new LocalCluster(vertx, container);
    cluster.shutdownNetwork(context);
    return this;
  }

  @Override
  public Vertigo shutdownLocalNetwork(NetworkContext context, Handler<AsyncResult<Void>> doneHandler) {
    Cluster cluster = new LocalCluster(vertx, container);
    cluster.shutdownNetwork(context, doneHandler);
    return this;
  }

  @Override
  public Vertigo deployRemoteNetwork(String address, Network network) {
    Cluster cluster = new RemoteCluster(vertx, container, address);
    cluster.deployNetwork(network);
    return this;
  }

  @Override
  public Vertigo deployRemoteNetwork(String address, Network network, Handler<AsyncResult<NetworkContext>> doneHandler) {
    Cluster cluster = new RemoteCluster(vertx, container, address);
    cluster.deployNetwork(network, doneHandler);
    return this;
  }

  @Override
  public Vertigo shutdownRemoteNetwork(String address, NetworkContext context) {
    Cluster cluster = new RemoteCluster(vertx, container, address);
    cluster.shutdownNetwork(context);
    return this;
  }

  @Override
  public Vertigo shutdownRemoteNetwork(String address, NetworkContext context, Handler<AsyncResult<Void>> doneHandler) {
    Cluster cluster = new RemoteCluster(vertx, container, address);
    cluster.shutdownNetwork(context, doneHandler);
    return this;
  }

}
