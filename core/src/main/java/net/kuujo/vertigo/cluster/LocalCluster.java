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

import java.util.UUID;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.Verticle;

import net.kuujo.vertigo.context.NetworkContext;
import net.kuujo.vertigo.context.impl.ContextBuilder;
import net.kuujo.vertigo.network.Network;

/**
 * A local cluster implementation.
 * <p>
 * 
 * The local cluster deploys Vertigo components (modules and verticles) within the local
 * Vert.x instance via the Vert.x {@link Container}.
 * 
 * @author Jordan Halterman
 */
public class LocalCluster extends AbstractCluster {
  private String deploymentID;

  public LocalCluster(String address, Verticle verticle) {
    super(address, verticle, LoggerFactory.getLogger(LocalCluster.class));
  }

  public LocalCluster(String address, Vertx vertx, Container container) {
    super(address, vertx, container, LoggerFactory.getLogger(LocalCluster.class));
  }

  @Override
  public void deployNetwork(Network network) {
    deployNetwork(network, null);
  }

  @Override
  public void deployNetwork(final Network network, final Handler<AsyncResult<NetworkContext>> doneHandler) {
    final NetworkContext context = ContextBuilder.buildContext(network);
    container.deployVerticle(VertigoClusterManager.class.getName(),
        new JsonObject().putString("cluster", address).putString("address", UUID.randomUUID().toString()),
        new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            if (result.failed()) {
              new DefaultFutureResult<NetworkContext>(result.cause()).setHandler(doneHandler);
            }
            else {
              deploymentID = result.result();
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
          }
        });
  }

  @Override
  public void shutdownNetwork(NetworkContext context) {
    shutdownNetwork(context, null);
  }

  @Override
  public void shutdownNetwork(NetworkContext context, final Handler<AsyncResult<Void>> doneHandler) {
    doUndeployNetwork(context, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          if (deploymentID != null) {
            container.undeployVerticle(deploymentID);
            deploymentID = null;
          }
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        }
        else if (deploymentID != null) {
          container.undeployVerticle(deploymentID, doneHandler);
          deploymentID = null;
        }
        else {
          new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
        }
      }
    });
  }

}