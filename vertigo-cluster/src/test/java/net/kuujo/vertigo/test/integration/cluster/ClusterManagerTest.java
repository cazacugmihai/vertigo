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
package net.kuujo.vertigo.test.integration.cluster;

import net.kuujo.vertigo.cluster.ClusterManager;
import net.kuujo.vertigo.cluster.VertigoNode;
import net.kuujo.vertigo.cluster.impl.DefaultClusterManager;

import org.junit.Test;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
import org.vertx.testtools.TestVerticle;

/**
 * A network cluster test.
 *
 * @author Jordan Halterman
 */
public class ClusterManagerTest extends TestVerticle {

  @Test
  public void testDeployVerticleToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        vertx.eventBus().registerHandler("foo", new Handler<Message<String>>() {
          @Override
          public void handle(Message<String> message) {
            if (message.body().equals("Hello world!")) {
              testComplete();
            }
          }
        }, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            final ClusterManager cluster = new DefaultClusterManager("test", vertx);
            cluster.deployVerticle("test", TestVerticle.class.getName(), new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testDeployVerticle() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        vertx.eventBus().registerHandler("foo", new Handler<Message<String>>() {
          @Override
          public void handle(Message<String> message) {
            if (message.body().equals("Hello world!")) {
              testComplete();
            }
          }
        }, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            final ClusterManager cluster = new DefaultClusterManager("test", vertx);
            cluster.deployVerticle("test", TestVerticle.class.getName(), new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testDeployVerticleResult() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.deployVerticle("test", TestVerticle.class.getName(), new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            assertTrue(result.succeeded());
            assertEquals("test", result.result());
          }
        });
      }
    });
  }

  public static class TestVerticle extends Verticle {
    @Override
    public void start() {
      vertx.eventBus().send("foo", "Hello world!");
    }
  }

  private void deployCluster(final int count, final Handler<AsyncResult<Void>> doneHandler) {
    deployCluster(0, count, doneHandler);
  }

  private void deployCluster(final int id, final int count, final Handler<AsyncResult<Void>> doneHandler) {
    if (id < count) {
      container.deployVerticle(VertigoNode.class.getName(), new JsonObject().putString("cluster", "test")
          .putString("address", String.format("test.%d", id+1)), new Handler<AsyncResult<String>>() {
        @Override
        public void handle(AsyncResult<String> result) {
          if (result.failed()) {
            new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
          }
          else {
            deployCluster(id+1, count, doneHandler);
          }
        }
      });
    }
    else {
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
    }
  }

}
