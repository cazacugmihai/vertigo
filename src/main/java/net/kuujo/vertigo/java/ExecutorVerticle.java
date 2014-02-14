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
package net.kuujo.vertigo.java;

import org.vertx.java.core.Handler;

import net.kuujo.vertigo.annotations.ExecutorOptions;
import net.kuujo.vertigo.component.ComponentFactory;
import net.kuujo.vertigo.component.impl.DefaultComponentFactory;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.rpc.Executor;

/**
 * An executor verticle implementation.
 *
 * @author Jordan Halterman
 */
@Deprecated
public abstract class ExecutorVerticle extends ComponentVerticle<Executor> {
  protected Executor executor;

  @Override
  protected Executor createComponent(InstanceContext<Executor> context) {
    ComponentFactory componentFactory = new DefaultComponentFactory(vertx, container);
    return componentFactory.createExecutor(context);
  }

  @Override
  protected void start(Executor executor) {
    this.executor = setupExecutor(executor);
    executor.executeHandler(new Handler<Executor>() {
      @Override
      public void handle(Executor executor) {
        nextMessage(executor);
      }
    });
  }

  /**
   * Sets up the executor according to executor options.
   */
  private Executor setupExecutor(Executor executor) {
    ExecutorOptions options = getClass().getAnnotation(ExecutorOptions.class);
    if (options != null) {
      executor.setResultTimeout(options.resultTimeout());
      executor.setExecuteQueueMaxSize(options.executeQueueMaxSize());
      executor.setAutoRetry(options.autoRetry());
      executor.setAutoRetryAttempts(options.autoRetryAttempts());
      executor.setExecuteInterval(options.executeInterval());
    }
    return executor;
  }

  /**
   * Called when the executor is requesting the next message.
   *
   * Override this method to perform polling-based executions. The executor will automatically
   * call this method any time the execute queue is prepared to accept new messages.
   */
  protected void nextMessage(Executor executor) {
  }

}
