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

import java.util.Collection;

import net.kuujo.vertigo.cluster.ClusterManager;
import net.kuujo.vertigo.cluster.ClusterManagerService;
import net.kuujo.vertigo.cluster.Event;
import net.kuujo.vertigo.cluster.VertigoClusterManager;
import net.kuujo.vertigo.cluster.impl.DefaultClusterManager;
import net.kuujo.vertigo.cluster.impl.DefaultClusterManagerService;

import org.junit.Test;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertNull;
import static org.vertx.testtools.VertxAssert.fail;
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
  public void testSetGetToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.get("test", new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                assertTrue(result.succeeded());
                assertEquals("Hello world!", result.result());
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testUpdateGetToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.set("test", "Hello world again!", new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.get("test", new Handler<AsyncResult<String>>() {
                  @Override
                  public void handle(AsyncResult<String> result) {
                    assertTrue(result.succeeded());
                    assertEquals("Hello world again!", result.result());
                    testComplete();
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testSetDeleteToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.get("test", new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                assertTrue(result.succeeded());
                assertEquals("Hello world!", result.result());
                cluster.delete("test", new Handler<AsyncResult<Boolean>>() {
                  @Override
                  public void handle(AsyncResult<Boolean> result) {
                    assertTrue(result.succeeded());
                    cluster.get("test", new Handler<AsyncResult<String>>() {
                      @Override
                      public void handle(AsyncResult<String> result) {
                        assertTrue(result.succeeded());
                        assertNull(result.result());
                        testComplete();
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testSetExistsToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.exists("test", new Handler<AsyncResult<Boolean>>() {
              @Override
              public void handle(AsyncResult<Boolean> result) {
                assertTrue(result.succeeded());
                assertTrue(result.result());
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testSetKeysToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test1", "Hello world1!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.set("test2", "Hello world2!", new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test3", "Hello world3!", new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                    cluster.keys(new Handler<AsyncResult<Collection<String>>>() {
                      @Override
                      public void handle(AsyncResult<Collection<String>> result) {
                        assertTrue(result.succeeded());
                        assertTrue(result.result().contains("test1"));
                        assertTrue(result.result().contains("test2"));
                        assertTrue(result.result().contains("test3"));
                        testComplete();
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testWatchCreateToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.delete("test", new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            assertTrue(result.succeeded());
            cluster.watch("test", Event.Type.CREATE, new Handler<Event>() {
              @Override
              public void handle(Event event) {
                assertEquals(Event.Type.CREATE, event.type());
                assertEquals("test", event.key());
                assertEquals("foo", event.value());
                testComplete();
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test", "foo");
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testWatchUpdateToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.watch("test", Event.Type.UPDATE, new Handler<Event>() {
              @Override
              public void handle(Event event) {
                assertEquals(Event.Type.UPDATE, event.type());
                assertEquals("test", event.key());
                assertEquals("Hello world again!", event.value());
                testComplete();
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test", "Hello world again!");
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testWatchDeleteToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.watch("test", Event.Type.DELETE, new Handler<Event>() {
              @Override
              public void handle(Event event) {
                assertEquals(Event.Type.DELETE, event.type());
                assertEquals("test", event.key());
                assertEquals("Hello world!", event.value());
                testComplete();
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.delete("test");
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testWatchUnwatchToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.watch("test", Event.Type.UPDATE, new Handler<Event>() {
              @Override
              public void handle(Event event) {
                assertEquals(Event.Type.UPDATE, event.type());
                assertEquals("test", event.key());
                assertEquals("Hello world again!", event.value());
                cluster.unwatch("test", Event.Type.UPDATE, this, new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                    cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
                      @Override
                      public void handle(AsyncResult<Void> result) {
                        assertTrue(result.succeeded());
                        testComplete();
                      }
                    });
                  }
                });
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test", "Hello world again!", new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testRecoverKeysToSingleNodeCluster() {
    container.deployVerticle(VertigoClusterManager.class.getName(), new JsonObject().putString("cluster", "test")
        .putString("address", String.format("test.1")), new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        final String deploymentID = result.result();
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test1", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.set("test2", "Hello world!", new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test3", "Hello world!", new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                    container.undeployVerticle(deploymentID, new Handler<AsyncResult<Void>>() {
                      @Override
                      public void handle(AsyncResult<Void> result) {
                        assertTrue(result.succeeded());
                        container.deployVerticle(VertigoClusterManager.class.getName(), new JsonObject().putString("cluster", "test")
                            .putString("address", String.format("test.1")), new Handler<AsyncResult<String>>() {
                          @Override
                          public void handle(AsyncResult<String> result) {
                            final ClusterManager cluster = new DefaultClusterManager("test", vertx);
                            cluster.get("test1", new Handler<AsyncResult<String>>() {
                              @Override
                              public void handle(AsyncResult<String> result) {
                                assertTrue(result.succeeded());
                                assertEquals("Hello world!", result.result());
                                cluster.get("test2", new Handler<AsyncResult<String>>() {
                                  @Override
                                  public void handle(AsyncResult<String> result) {
                                    assertTrue(result.succeeded());
                                    assertEquals("Hello world!", result.result());
                                    cluster.get("test3", new Handler<AsyncResult<String>>() {
                                      @Override
                                      public void handle(AsyncResult<String> result) {
                                        assertTrue(result.succeeded());
                                        assertEquals("Hello world!", result.result());
                                        testComplete();
                                      }
                                    });
                                  }
                                });
                              }
                            });
                          }
                        });
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testRecoverKeysViaSnapshotToSingleNodeCluster() {
    container.deployVerticle(VertigoClusterManager.class.getName(), new JsonObject().putString("cluster", "test")
        .putString("address", String.format("test.1")).putNumber("max_log_size", 1028), new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        final String deploymentID = result.result();
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        setKeys(cluster, "test", 100, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              fail(result.cause().getMessage());
            }

            container.undeployVerticle(deploymentID, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                container.deployVerticle(VertigoClusterManager.class.getName(), new JsonObject().putString("cluster", "test")
                  .putString("address", String.format("test.1")).putNumber("max_log_size", 1028), new Handler<AsyncResult<String>>() {
                    @Override
                    public void handle(AsyncResult<String> result) {
                      assertTrue(result.succeeded());
                      cluster.get("test1", new Handler<AsyncResult<String>>() {
                        @Override
                        public void handle(AsyncResult<String> result) {
                          if (result.failed()) {
                            fail(result.cause().getMessage());
                          }
                          assertEquals("Hello world1!", result.result());
                          testComplete();
                        }
                      });
                    }
                });
              }
            });
          }
        });
      }
    });
  }

  private void setKeys(ClusterManager cluster, String prefix, int total, Handler<AsyncResult<Void>> doneHandler) {
    setKeys(cluster, prefix, 0, total, doneHandler);
  }

  private void setKeys(final ClusterManager cluster, final String prefix, final int count, final int total, final Handler<AsyncResult<Void>> doneHandler) {
    if (count < total) {
      cluster.set(String.format("%s%s", prefix, count+1), String.format("Hello world%d!", count+1), new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
          }
          else {
            setKeys(cluster, prefix, count+1, total, doneHandler);
          }
        }
      });
    }
    else {
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
    }
  }

  @Test
  public void testSetGetToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.get("test", new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                assertTrue(result.succeeded());
                assertEquals("Hello world!", result.result());
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testUpdateGetToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.set("test", "Hello world again!", new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.get("test", new Handler<AsyncResult<String>>() {
                  @Override
                  public void handle(AsyncResult<String> result) {
                    assertTrue(result.succeeded());
                    assertEquals("Hello world again!", result.result());
                    testComplete();
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testSetDeleteToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.get("test", new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                assertTrue(result.succeeded());
                assertEquals("Hello world!", result.result());
                cluster.delete("test", new Handler<AsyncResult<Boolean>>() {
                  @Override
                  public void handle(AsyncResult<Boolean> result) {
                    assertTrue(result.succeeded());
                    cluster.get("test", new Handler<AsyncResult<String>>() {
                      @Override
                      public void handle(AsyncResult<String> result) {
                        assertTrue(result.succeeded());
                        assertNull(result.result());
                        testComplete();
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testSetExistsToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.exists("test", new Handler<AsyncResult<Boolean>>() {
              @Override
              public void handle(AsyncResult<Boolean> result) {
                assertTrue(result.succeeded());
                assertTrue(result.result());
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testSetKeysToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test1", "Hello world1!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.set("test2", "Hello world2!", new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test3", "Hello world3!", new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                    cluster.keys(new Handler<AsyncResult<Collection<String>>>() {
                      @Override
                      public void handle(AsyncResult<Collection<String>> result) {
                        assertTrue(result.succeeded());
                        assertTrue(result.result().contains("test1"));
                        assertTrue(result.result().contains("test2"));
                        assertTrue(result.result().contains("test3"));
                        testComplete();
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testWatchCreateToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.delete("test", new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            assertTrue(result.succeeded());
            cluster.watch("test", Event.Type.CREATE, new Handler<Event>() {
              @Override
              public void handle(Event event) {
                assertEquals(Event.Type.CREATE, event.type());
                assertEquals("test", event.key());
                assertEquals("foo", event.value());
                testComplete();
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test", "foo");
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testWatchUpdateToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.watch("test", Event.Type.UPDATE, new Handler<Event>() {
              @Override
              public void handle(Event event) {
                assertEquals(Event.Type.UPDATE, event.type());
                assertEquals("test", event.key());
                assertEquals("Hello world again!", event.value());
                testComplete();
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test", "Hello world again!");
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testWatchDeleteToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.watch("test", Event.Type.DELETE, new Handler<Event>() {
              @Override
              public void handle(Event event) {
                assertEquals(Event.Type.DELETE, event.type());
                assertEquals("test", event.key());
                assertEquals("Hello world!", event.value());
                testComplete();
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.delete("test");
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testWatchUnwatchToMultiNodeCluster() {
    deployCluster(3, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            cluster.watch("test", Event.Type.UPDATE, new Handler<Event>() {
              @Override
              public void handle(Event event) {
                assertEquals(Event.Type.UPDATE, event.type());
                assertEquals("test", event.key());
                assertEquals("Hello world again!", event.value());
                cluster.unwatch("test", Event.Type.UPDATE, this, new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                    cluster.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
                      @Override
                      public void handle(AsyncResult<Void> result) {
                        assertTrue(result.succeeded());
                        testComplete();
                      }
                    });
                  }
                });
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                cluster.set("test", "Hello world again!", new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                  }
                });
              }
            });
          }
        });
      }
    });
  }

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
  public void testDeployVerticleResultToSingleNodeCluster() {
    deployCluster(1, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final ClusterManager cluster = new DefaultClusterManager("test", vertx);
        cluster.deployVerticle("test", TestVerticle.class.getName(), new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            assertTrue(result.succeeded());
            assertEquals("test", result.result());
            testComplete();
          }
        });
      }
    });
  }

  @Test
  public void testDeployVerticleToMultiNodeCluster() {
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
  public void testDeployVerticleResultToMultiNodeCluster() {
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
            testComplete();
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
      ClusterManagerService service = new DefaultClusterManagerService(String.format("test.%d", id+1), "test", vertx, container);
      service.start(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
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
