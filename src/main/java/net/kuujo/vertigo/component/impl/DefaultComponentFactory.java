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
package net.kuujo.vertigo.component.impl;

import net.kuujo.vertigo.component.Component;
import net.kuujo.vertigo.component.ComponentFactory;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.feeder.Feeder;
import net.kuujo.vertigo.feeder.impl.BasicFeeder;
import net.kuujo.vertigo.rpc.Executor;
import net.kuujo.vertigo.worker.Worker;
import net.kuujo.vertigo.worker.impl.BasicWorker;

import org.vertx.java.core.Vertx;
import org.vertx.java.platform.Container;

/**
 * A default component factory implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultComponentFactory implements ComponentFactory {
  private Vertx vertx;
  private Container container;

  public DefaultComponentFactory() {
  }

  public DefaultComponentFactory(Vertx vertx, Container container) {
    setVertx(vertx);
    setContainer(container);
  }

  @Override
  public ComponentFactory setVertx(Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  @Override
  public ComponentFactory setContainer(Container container) {
    this.container = container;
    return this;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public <T extends Component<T>> T createComponent(InstanceContext context) {
    net.kuujo.vertigo.network.Component.Type type = context.componentContext().type();
    if (type.equals(net.kuujo.vertigo.network.Component.Type.FEEDER)) {
      return (T) createFeeder(context);
    }
    else if (type.equals(net.kuujo.vertigo.network.Component.Type.WORKER)) {
      return (T) createWorker(context);
    }
    else {
      throw new IllegalArgumentException("Invalid component type.");
    }
  }

  @Override
  public Feeder createFeeder(InstanceContext context) {
    if (!context.componentContext().type().equals(net.kuujo.vertigo.network.Component.Type.FEEDER)) {
      throw new IllegalArgumentException("Not a valid feeder context.");
    }
    return new BasicFeeder(vertx, container, context);
  }

  @Override
  public Executor createExecutor(InstanceContext context) {
    throw new IllegalArgumentException("Not a valid executor context.");
  }

  @Override
  public Worker createWorker(InstanceContext context) {
    if (!context.componentContext().type().equals(net.kuujo.vertigo.network.Component.Type.WORKER)) {
      throw new IllegalArgumentException("Not a valid worker context.");
    }
    return new BasicWorker(vertx, container, context);
  }

}
