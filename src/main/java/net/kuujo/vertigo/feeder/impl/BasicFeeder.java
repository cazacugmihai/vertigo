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
package net.kuujo.vertigo.feeder.impl;

import java.util.HashMap;
import java.util.Map;

import net.kuujo.vertigo.annotations.Factory;
import net.kuujo.vertigo.component.impl.AbstractComponent;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.feeder.Feeder;
import net.kuujo.vertigo.message.MessageId;
import net.kuujo.vertigo.runtime.FailureException;
import net.kuujo.vertigo.runtime.TimeoutException;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

/**
 * An basic feeder implementation.
 *
 * @author Jordan Halterman
 */
public class BasicFeeder extends AbstractComponent<Feeder> implements Feeder {

  /**
   * Constant indicating unlimited automatic retry attempts.
   */
  public static final int AUTO_RETRY_ATTEMPTS_UNLIMITED = -1;

  @Factory
  public static BasicFeeder factory(Vertx vertx, Container container, InstanceContext context) {
    return new BasicFeeder(vertx, container, context);
  }

  private static final long DEFAULT_FEED_INTERVAL = 10;
  private Handler<Feeder> feedHandler;
  private Handler<Void> drainHandler;
  private InternalQueue queue = new InternalQueue();
  private boolean autoRetry;
  private int retryAttempts = AUTO_RETRY_ATTEMPTS_UNLIMITED;
  private long feedDelay = DEFAULT_FEED_INTERVAL;
  private boolean started;
  private boolean paused;
  private boolean fed;
  private long feedTimer;

  public BasicFeeder(Vertx vertx, Container container, InstanceContext context) {
    super(vertx, container, context);
  }

  private final Handler<MessageId> internalAckHandler = new Handler<MessageId>() {
    @Override
    public void handle(MessageId id) {
      queue.ack(id);
    }
  };

  private final Handler<MessageId> internalFailHandler = new Handler<MessageId>() {
    @Override
    public void handle(MessageId id) {
      queue.fail(id);
    }
  };

  private final Handler<MessageId> internalTimeoutHandler = new Handler<MessageId>() {
    @Override
    public void handle(MessageId id) {
      queue.timeout(id);
    }
  };

  @Override
  public Feeder start() {
    return super.start(new Handler<AsyncResult<Feeder>>() {
      @Override
      public void handle(AsyncResult<Feeder> result) {
        if (result.succeeded()) {
          recursiveFeed();
        }
      }
    });
  }

  @Override
  public Feeder start(Handler<AsyncResult<Feeder>> doneHandler) {
    output.ackHandler(internalAckHandler);
    output.failHandler(internalFailHandler);
    output.timeoutHandler(internalTimeoutHandler);
    final Future<Feeder> future = new DefaultFutureResult<Feeder>().setHandler(doneHandler);
    return super.start(new Handler<AsyncResult<Feeder>>() {
      @Override
      public void handle(AsyncResult<Feeder> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          future.setResult(result.result());
          started = true;
          recursiveFeed();
        }
      }
    });
  }

  /**
   * Recursively invokes the feed handler.
   * If the feed handler is invoked and no messages are fed from the handler,
   * a timer is set to restart the feed in the future.
   */
  private void recursiveFeed() {
    if (feedHandler != null) {
      fed = true;
      while (fed && !feedQueueFull()) {
        fed = false;
        feedHandler.handle(this);
      }

      feedTimer = vertx.setTimer(feedDelay, new Handler<Long>() {
        @Override
        public void handle(Long timerID) {
          recursiveFeed();
        }
      });
    }
    else if (feedTimer > 0) {
      vertx.cancelTimer(feedTimer);
      feedTimer = 0;
    }
  }

  @Override
  public Feeder setFeedQueueMaxSize(long maxSize) {
    queue.maxSize = maxSize;
    return this;
  }

  @Override
  public long getFeedQueueMaxSize() {
    return queue.maxSize;
  }

  @Override
  public boolean feedQueueFull() {
    return queue.full();
  }

  @Override
  public Feeder setAutoRetry(boolean retry) {
    autoRetry = retry;
    return this;
  }

  @Override
  public boolean isAutoRetry() {
    return autoRetry;
  }

  @Override
  public Feeder setAutoRetryAttempts(int attempts) {
    retryAttempts = attempts;
    return this;
  }

  @Override
  public int getAutoRetryAttempts() {
    return retryAttempts;
  }

  @Override
  public Feeder setFeedDelay(long delay) {
    feedDelay = delay;
    return this;
  }

  @Override
  public long getFeedDelay() {
    return feedDelay;
  }


  @Override
  public Feeder feedHandler(Handler<Feeder> feedHandler) {
    this.feedHandler = feedHandler;
    if (started) {
      recursiveFeed();
    }
    return this;
  }

  @Override
  public Feeder drainHandler(Handler<Void> drainHandler) {
    this.drainHandler = drainHandler;
    return this;
  }

  @Override
  public MessageId emit(JsonObject data) {
    return doFeed(null, data, 0, null);
  }

  @Override
  public MessageId emit(JsonObject data, Handler<AsyncResult<MessageId>> ackHandler) {
    return doFeed(null, data, 0, ackHandler);
  }

  @Override
  public MessageId emit(String stream, JsonObject data) {
    return doFeed(stream, data, 0, null);
  }

  @Override
  public MessageId emit(String stream, JsonObject data, Handler<AsyncResult<MessageId>> ackHandler) {
    return doFeed(stream, data, 0, ackHandler);
  }

  /**
   * Executes a feed.
   */
  protected final MessageId doFeed(final String stream, final JsonObject data, final Handler<AsyncResult<MessageId>> ackHandler) {
    return doFeed(stream, data, 0, ackHandler);
  }

  /**
   * Executes a feed.
   */
  protected final MessageId doFeed(final JsonObject data, final Handler<AsyncResult<MessageId>> ackHandler) {
    return doFeed(null, data, 0, ackHandler);
  }

  /**
   * Executes a feed.
   */
  private final MessageId doFeed(final String stream, final JsonObject data, final int attempts, final Handler<AsyncResult<MessageId>> ackHandler) {
    final MessageId id = stream != null ? output.emitTo(stream, data) : output.emit(data);
    queue.enqueue(id, new Handler<AsyncResult<MessageId>>() {
      @Override
      public void handle(AsyncResult<MessageId> result) {
        if (autoRetry && (retryAttempts == AUTO_RETRY_ATTEMPTS_UNLIMITED || attempts < retryAttempts)
            && result.failed() && result.cause() instanceof TimeoutException) {
          doFeed(stream, data, attempts+1, ackHandler);
        }
        else if (ackHandler != null) {
          ackHandler.handle(result);
        }
      }
    });
    fed = true; checkPause();
    return id;
  }

  /**
   * Checks the current pause status.
   */
  private void checkPause() {
    if (paused) {
      if (!feedQueueFull()) {
        paused = false;
        if (drainHandler != null) {
          drainHandler.handle(null);
        }
      }
    }
    else if (feedQueueFull()) {
      paused = true;
    }
  }

  /**
   * A future that may contain a result even if the future is failed.
   */
  private static class InternalFutureResult<T> extends DefaultFutureResult<T> {
    private boolean complete;

    @Override
    public boolean complete() {
      return complete;
    }

    private void complete(T result) {
      complete = true;
      setResult(result);
    }
  }

  /**
   * An internal feeder queue.
   */
  private static class InternalQueue {
    // Set up failure and timeout exceptions without stack traces. This prevents
    // us from having to create exceptions repeatedly which would otherwise result
    // in the stack trace being filled in frequently.
    private static final FailureException FAILURE_EXCEPTION = new FailureException("Processing failed.");
    static { FAILURE_EXCEPTION.setStackTrace(new StackTraceElement[0]); }

    private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException("Processing timed out.");
    static { TIMEOUT_EXCEPTION.setStackTrace(new StackTraceElement[0]); }

    private final Map<String, InternalFutureResult<MessageId>> handlers = new HashMap<String, InternalFutureResult<MessageId>>();
    private long maxSize = 1000;

    /**
     * Returns the queue size.
     */
    private int size() {
      return handlers.size();
    }

    /**
     * Returns a boolean indicating whether the queue is full.
     */
    private boolean full() {
      return size() >= maxSize;
    }

    /**
     * Enqueues a new item.
     */
    private void enqueue(MessageId id, Handler<AsyncResult<MessageId>> ackHandler) {
      InternalFutureResult<MessageId> future = new InternalFutureResult<MessageId>();
      future.setHandler(ackHandler);
      handlers.put(id.correlationId(), future);
    }

    /**
     * Acks an item in the queue. The item will be removed and its ack handler called.
     */
    private void ack(MessageId id) {
      InternalFutureResult<MessageId> future = handlers.remove(id.correlationId());
      if (future != null) {
        future.complete(id);
      }
    }

    /**
     * Fails an item in the queue. The item will be removed and its fail handler called.
     */
    private void fail(MessageId id) {
      InternalFutureResult<MessageId> future = handlers.remove(id.correlationId());
      if (future != null) {
        future.setFailure(FAILURE_EXCEPTION);
        future.complete(id);
      }
    }

    /**
     * Times out an item in the queue. The item will be removed and its timeout handler called.
     */
    private void timeout(MessageId id) {
      InternalFutureResult<MessageId> future = handlers.remove(id.correlationId());
      if (future != null) {
        future.setFailure(TIMEOUT_EXCEPTION);
        future.complete(id);
      }
    }
  }

}
