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
import java.util.Iterator;

import net.kuujo.vertigo.auditor.AuditorVerticle;
import net.kuujo.vertigo.cluster.impl.DefaultClusterManager;
import net.kuujo.vertigo.context.ComponentContext;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.context.ModuleContext;
import net.kuujo.vertigo.context.NetworkContext;
import net.kuujo.vertigo.context.VerticleContext;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.Verticle;

/**
 * An abstract cluster.
 * 
 * @author Jordan Halterman
 */
abstract class AbstractCluster implements Cluster {
  protected final String address;
  protected final Vertx vertx;
  protected final Container container;
  protected final Logger logger;
  protected final ClusterManager cluster;

  protected AbstractCluster(String address, Verticle verticle, Logger logger) {
    this.address = address;
    this.vertx = verticle.getVertx();
    this.container = verticle.getContainer();
    this.logger = logger;
    this.cluster = new DefaultClusterManager(address, vertx);
  }

  protected AbstractCluster(String address, Vertx vertx, Container container, Logger logger) {
    this.address = address;
    this.vertx = vertx;
    this.container = container;
    this.logger = logger;
    this.cluster = new DefaultClusterManager(address, vertx);
  }

  @Override
  public String address() {
    return address;
  }

  /**
   * Deploys the network.
   */
  protected void doDeployNetwork(final NetworkContext context, final Handler<AsyncResult<Void>> doneHandler) {
    doDeployNetwork(context, new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  /**
   * Iteratively deploys the network.
   */
  private void doDeployNetwork(final NetworkContext context, final Future<Void> future) {
    logger.info(String.format("Deploying network '%s'", context.address()));
    doDeployAuditors(context.address(), context.auditors(), context.messageTimeout(), new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          doDeployComponents(context.components(), new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                future.setResult((Void) null);
              }
            }
          });
        }
      }
    });
  }

  /**
   * Deploys all network auditors.
   */
  private void doDeployAuditors(final String network, final Collection<String> auditors, final long messageTimeout,
      final Handler<AsyncResult<Void>> doneHandler) {
    doDeployAuditors(network, auditors.iterator(), messageTimeout, new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  /**
   * Iteratively deploys network auditors.
   */
  private void doDeployAuditors(final String network, final Iterator<String> iterator, final long messageTimeout, final Future<Void> future) {
    if (iterator.hasNext()) {
      String address = iterator.next();
      logger.info(String.format("Deploying auditor '%s'", address));
      cluster.deployVerticle(address, AuditorVerticle.class.getName(),
          new JsonObject().putString("address", address).putNumber("timeout", messageTimeout), new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                doDeployAuditors(network, iterator, messageTimeout, future);
              }
            }
          });
    }
    else {
      future.setResult((Void) null);
    }
  }

  /**
   * Deploys all network components.
   */
  private void doDeployComponents(final Collection<ComponentContext<?>> components, final Handler<AsyncResult<Void>> doneHandler) {
    doDeployComponents(components.iterator(), new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  /**
   * Iteratively deploys network components.
   */
  private void doDeployComponents(final Iterator<ComponentContext<?>> iterator, final Future<Void> future) {
    if (iterator.hasNext()) {
      ComponentContext<?> context = iterator.next();
      logger.info(String.format("Deploying component '%s'", context.address()));
      doDeployInstances(context.instances(), new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            future.setFailure(result.cause());
          }
          else {
            doDeployComponents(iterator, future);
          }
        }
      });
    }
    else {
      future.setResult((Void) null);
    }
  }

  /**
   * Deploys all component instances.
   */
  private void doDeployInstances(final Collection<InstanceContext> instances, final Handler<AsyncResult<Void>> doneHandler) {
    doDeployInstances(instances.iterator(), new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  /**
   * Iteratively deploys component instances.
   */
  private void doDeployInstances(final Iterator<InstanceContext> iterator, final Future<Void> future) {
    if (iterator.hasNext()) {
      InstanceContext context = iterator.next();
      logger.info(String.format("Deploying '%s' instance %d", context.component().address(), context.number()));
      if (context.component().isModule()) {
        cluster.deployModule(context.address(), context.<ModuleContext> component().module(), InstanceContext.toJson(context),
            new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                if (result.failed()) {
                  future.setFailure(result.cause());
                }
                else {
                  doDeployInstances(iterator, future);
                }
              }
            });
      }
      else if (((VerticleContext) context.component()).isWorker()) {
        cluster.deployWorkerVerticle(context.address(), context.<VerticleContext> component().main(),
            InstanceContext.toJson(context), context.<VerticleContext> component().isMultiThreaded(),
            new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                if (result.failed()) {
                  future.setFailure(result.cause());
                }
                else {
                  doDeployInstances(iterator, future);
                }
              }
            });
      }
      else {
        cluster.deployVerticle(context.address(), context.<VerticleContext> component().main(), InstanceContext.toJson(context),
            new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                if (result.failed()) {
                  future.setFailure(result.cause());
                }
                else {
                  doDeployInstances(iterator, future);
                }
              }
            });
      }
    }
    else {
      future.setResult((Void) null);
    }
  }

  /**
   * Undeploys the network.
   */
  protected void doUndeployNetwork(final NetworkContext context, final Handler<AsyncResult<Void>> doneHandler) {
    doUndeployNetwork(context, new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  /**
   * Iteratively undeploys the network.
   */
  private void doUndeployNetwork(final NetworkContext context, final Future<Void> future) {
    logger.info(String.format("Undeploying network '%s'", context.address()));
    doUndeployAuditors(context.address(), context.auditors(), context.messageTimeout(), new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          doUndeployComponents(context.components(), new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                future.setResult((Void) null);
              }
            }
          });
        }
      }
    });
  }

  /**
   * Undeploys all network auditors.
   */
  private void doUndeployAuditors(final String network, final Collection<String> auditors, final long messageTimeout,
      final Handler<AsyncResult<Void>> doneHandler) {
    doUndeployAuditors(network, auditors.iterator(), messageTimeout, new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  /**
   * Iteratively undeploys network auditors.
   */
  private void doUndeployAuditors(final String network, final Iterator<String> iterator, final long messageTimeout,
      final Future<Void> future) {
    if (iterator.hasNext()) {
      String address = iterator.next();
      logger.info(String.format("Undeploying auditor '%s'", address));
      cluster.undeployVerticle(address, new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            future.setFailure(result.cause());
          }
          else {
            doUndeployAuditors(network, iterator, messageTimeout, future);
          }
        }
      });
    }
    else {
      future.setResult((Void) null);
    }
  }

  /**
   * Undeploys all network components.
   */
  private void doUndeployComponents(final Collection<ComponentContext<?>> components, final Handler<AsyncResult<Void>> doneHandler) {
    doUndeployComponents(components.iterator(), new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  /**
   * Iteratively undeploys network components.
   */
  private void doUndeployComponents(final Iterator<ComponentContext<?>> iterator, final Future<Void> future) {
    if (iterator.hasNext()) {
      ComponentContext<?> context = iterator.next();
      logger.info(String.format("Undeploying component '%s'", context.address()));
      doUndeployInstances(context.instances(), new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            future.setFailure(result.cause());
          }
          else {
            doUndeployComponents(iterator, future);
          }
        }
      });
    }
    else {
      future.setResult((Void) null);
    }
  }

  /**
   * Undeploys all component instances.
   */
  private void doUndeployInstances(final Collection<InstanceContext> instances, final Handler<AsyncResult<Void>> doneHandler) {
    doUndeployInstances(instances.iterator(), new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  /**
   * Iteratively undeploys component instances.
   */
  private void doUndeployInstances(final Iterator<InstanceContext> iterator, final Future<Void> future) {
    if (iterator.hasNext()) {
      InstanceContext context = iterator.next();
      logger.info(String.format("Undeploying '%s' instance %d", context.component().address(), context.number()));
      if (context.component().isModule()) {
        cluster.undeployModule(context.address(), new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              future.setFailure(result.cause());
            }
            else {
              doUndeployInstances(iterator, future);
            }
          }
        });
      }
      else {
        cluster.undeployVerticle(context.address(), new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              future.setFailure(result.cause());
            }
            else {
              doUndeployInstances(iterator, future);
            }
          }
        });
      }
    }
    else {
      future.setResult((Void) null);
    }
  }

}