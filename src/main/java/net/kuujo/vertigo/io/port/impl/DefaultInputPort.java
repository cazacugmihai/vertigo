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
package net.kuujo.vertigo.io.port.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.kuujo.vertigo.context.InputConnectionContext;
import net.kuujo.vertigo.context.InputPortContext;
import net.kuujo.vertigo.io.connection.InputConnection;
import net.kuujo.vertigo.io.connection.impl.DefaultInputConnection;
import net.kuujo.vertigo.io.group.InputGroup;
import net.kuujo.vertigo.io.port.InputPort;
import net.kuujo.vertigo.util.CountingCompletionHandler;
import net.kuujo.vertigo.util.Observer;
import net.kuujo.vertigo.util.Task;
import net.kuujo.vertigo.util.TaskRunner;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * Default input port implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultInputPort implements InputPort, Observer<InputPortContext> {
  private static final Logger log = LoggerFactory.getLogger(DefaultInputPort.class);
  private final Vertx vertx;
  private InputPortContext context;
  private final List<InputConnection> connections = new ArrayList<>();
  private final TaskRunner tasks = new TaskRunner();
  @SuppressWarnings("rawtypes")
  private Handler messageHandler;
  private final Map<String, Handler<InputGroup>> groupHandlers = new HashMap<>();
  private boolean open;
  private boolean paused;

  public DefaultInputPort(Vertx vertx, InputPortContext context) {
    this.vertx = vertx;
    this.context = context;
    context.registerObserver(this);
  }

  @Override
  public String name() {
    return context.name();
  }

  @Override
  public Vertx vertx() {
    return vertx;
  }

  @Override
  public InputPortContext context() {
    return context;
  }

  @Override
  public void update(final InputPortContext update) {
    // All updates are run sequentially to prevent race conditions
    // during configuration changes. Without essentially locking the
    // object, it could be possible that connections are simultaneously
    // added and removed or opened and closed on the object.
    tasks.runTask(new Handler<Task>() {
      @Override
      public void handle(final Task task) {
        Iterator<InputConnection> iter = connections.iterator();
        while (iter.hasNext()) {
          final InputConnection connection = iter.next();
          boolean exists = false;
          for (InputConnectionContext input : update.connections()) {
            if (input.equals(connection.context())) {
              exists = true;
              break;
            }
          }
          if (!exists) {
            connection.close(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  log.error("Failed to close input connection " + connection.address());
                }
              }
            });
            iter.remove();
          }
        }

        final List<InputConnection> newConnections = new ArrayList<>();
        for (InputConnectionContext input : update.connections()) {
          boolean exists = false;
          for (InputConnection connection : connections) {
            if (connection.context().equals(input)) {
              exists = true;
              break;
            }
          }
          if (!exists) {
            newConnections.add(new DefaultInputConnection(vertx, input));
          }
        }

        if (open) {
          final CountingCompletionHandler<Void> counter = new CountingCompletionHandler<Void>(newConnections.size());
          counter.setHandler(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              task.complete();
            }
          });
          for (final InputConnection connection : newConnections) {
            connection.messageHandler(messageHandler);
            for (Map.Entry<String, Handler<InputGroup>> entry : groupHandlers.entrySet()) {
              connection.groupHandler(entry.getKey(), entry.getValue());
            }
            if (paused) {
              connection.pause();
            }
            connection.open(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  log.error("Failed to open input connection " + connection.address());
                } else {
                  connections.add(connection);
                }
              }
            });
          }
        } else {
          for (InputConnection connection : newConnections) {
            connection.messageHandler(messageHandler);
            for (Map.Entry<String, Handler<InputGroup>> entry : groupHandlers.entrySet()) {
              connection.groupHandler(entry.getKey(), entry.getValue());
            }
            if (paused) {
              connection.pause();
            }
            connections.add(connection);
          }
          task.complete();
        }
      }
    });
  }

  @Override
  public InputPort pause() {
    paused = true;
    for (InputConnection connection : connections) {
      connection.pause();
    }
    return this;
  }

  @Override
  public InputPort resume() {
    paused = false;
    for (InputConnection connection : connections) {
      connection.resume();
    }
    return this;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public InputPort messageHandler(Handler handler) {
    this.messageHandler = handler;
    for (InputConnection connection : connections) {
      connection.messageHandler(messageHandler);
    }
    return this;
  }

  @Override
  public InputPort groupHandler(String group, Handler<InputGroup> handler) {
    this.groupHandlers.put(group, handler);
    for (InputConnection connection : connections) {
      connection.groupHandler(group, handler);
    }
    return this;
  }

  @Override
  public InputPort open() {
    return open(null);
  }

  @Override
  public InputPort open(final Handler<AsyncResult<Void>> doneHandler) {
    // Prevent the object from being opened and closed simultaneously
    // by queueing open/close operations as tasks.
    tasks.runTask(new Handler<Task>() {
      @Override
      public void handle(final Task task) {
        if (!open) {
          final CountingCompletionHandler<Void> startCounter = new CountingCompletionHandler<Void>(context.connections().size());
          startCounter.setHandler(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              } else {
                open = true;
                new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
              }
              task.complete();
            }
          });

          connections.clear();
          for (InputConnectionContext connectionContext : context.connections()) {
            final InputConnection connection = new DefaultInputConnection(vertx, connectionContext);
            connection.messageHandler(messageHandler);
            for (Map.Entry<String, Handler<InputGroup>> entry : groupHandlers.entrySet()) {
              connection.groupHandler(entry.getKey(), entry.getValue());
            }
            if (paused) {
              connection.pause();
            }
            connection.open(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  log.error("Failed to open input connection " + connection.address());
                  startCounter.fail(result.cause());
                } else {
                  connections.add(connection);
                  startCounter.succeed();
                }
              }
            });
          }
        } else {
          new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          task.complete();
        }
      }
    });
    return this;
  }

  @Override
  public void close() {
    close(null);
  }

  @Override
  public void close(final Handler<AsyncResult<Void>> doneHandler) {
    // Prevent the object from being opened and closed simultaneously
    // by queueing open/close operations as tasks.
    tasks.runTask(new Handler<Task>() {
      @Override
      public void handle(final Task task) {
        if (open) {
          final CountingCompletionHandler<Void> stopCounter = new CountingCompletionHandler<Void>(connections.size());
          stopCounter.setHandler(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              } else {
                connections.clear();
                new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
              }
              task.complete();
            }
          });

          for (InputConnection connection : connections) {
            connection.close(stopCounter);
          }
        } else {
          new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          task.complete();
        }
      }
    });
  }

}