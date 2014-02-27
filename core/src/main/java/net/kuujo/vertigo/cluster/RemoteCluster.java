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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.Verticle;

import net.kuujo.vertigo.context.NetworkContext;
import net.kuujo.vertigo.context.impl.ContextBuilder;
import net.kuujo.vertigo.network.Network;

/**
 * A remote cluster implementation.
 * <p>
 * 
 * The remote cluster supports deploying networks across a cluster of Vert.x instances
 * using an event bus based deployment mechanism. Rather than deploying modules and
 * verticles using the Vert.x container, Via sends messages to supervisors on different
 * Vert.x instances in a cluster, with each supervisor deploying and monitoring modules
 * and verticles within its own instance. This results in a much more reliable network
 * deployment as Via can reassign deployments to new nodes when an existing node dies. See
 * the Via documentation for deployment message structures.
 * 
 * @author Jordan Halterman
 */
public class RemoteCluster extends AbstractCluster {

  public RemoteCluster(String address, Verticle verticle) {
    super(address, verticle, LoggerFactory.getLogger(RemoteCluster.class));
  }

  public RemoteCluster(String address, Vertx vertx, Container container) {
    super(address, vertx, container, LoggerFactory.getLogger(RemoteCluster.class));
  }

  @Override
  public void deployNetwork(Network network) {
    deployNetwork(network, null);
  }

  @Override
  public void deployNetwork(Network network, final Handler<AsyncResult<NetworkContext>> doneHandler) {
    final NetworkContext context = ContextBuilder.buildContext(network);
    doDeployNetwork(context, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<NetworkContext>(result.cause()).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<NetworkContext>(context).setHandler(doneHandler);
        }
      }
    });
  }

  @Override
  public void shutdownNetwork(NetworkContext context) {
    shutdownNetwork(context, null);
  }

  @Override
  public void shutdownNetwork(NetworkContext context, Handler<AsyncResult<Void>> doneHandler) {
    doUndeployNetwork(context, doneHandler);
  }

}