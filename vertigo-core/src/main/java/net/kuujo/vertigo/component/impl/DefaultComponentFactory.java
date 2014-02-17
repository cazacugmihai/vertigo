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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import net.kuujo.vertigo.annotations.Factory;
import net.kuujo.vertigo.component.Component;
import net.kuujo.vertigo.component.ComponentFactory;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.feeder.Feeder;
import net.kuujo.vertigo.worker.Worker;

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
  @SuppressWarnings("unchecked")
  public <T extends Component<?>> T createComponent(Class<T> type, InstanceContext context) {
    // Validate a feeder.
    if (Feeder.class.isAssignableFrom(type)) {
      if (!context.componentContext().type().equals(net.kuujo.vertigo.network.Component.Type.FEEDER)) {
        throw new IllegalArgumentException(type.getCanonicalName() + " is not a valid feeder component.");
      }
    }

    // Validate a worker.
    if (Worker.class.isAssignableFrom(type)) {
      if (!context.componentContext().type().equals(net.kuujo.vertigo.network.Component.Type.WORKER)) {
        throw new IllegalArgumentException(type.getCanonicalName() + " is not a valid worker component.");
      }
    }

    // Search the class for a factory method.
    for (Method method : type.getDeclaredMethods()) {
      if (method.isAnnotationPresent(Factory.class)) {
        // The method must be public static.
        if (!method.isAccessible() || !Modifier.isStatic(method.getModifiers())) {
          throw new IllegalArgumentException("Factory method " + method.getName() + " in " + type.getCanonicalName() + " must be public state.");
        }
        // The method return type must be a Class<T> instance.
        if (!method.getReturnType().equals(type)) {
          throw new IllegalArgumentException("Factory method " + method.getName() + " in " + type.getCanonicalName() + " must return a " + type.getCanonicalName() + " instance.");
        }

        // Set up the factory arguments.
        Class<?>[] params = method.getParameterTypes();
        List<Object> args = new ArrayList<>();
        for (int i = 0; i < params.length; i++) {
          args.set(i, null);
          if (Vertx.class.isAssignableFrom(params[i])) {
            args.set(i, vertx);
          }
          else if (Container.class.isAssignableFrom(params[i])) {
            args.set(i, container);
          }
          else if (InstanceContext.class.isAssignableFrom(params[i])) {
            args.set(i, context);
          }
        }

        // Invoke the factory method.
        try {
          return (T) method.invoke(null, args.toArray());
        }
        catch (IllegalAccessException | InvocationTargetException e) {
          continue; // Just skip it. An exception will be thrown later.
        }
      }
    }

    throw new IllegalArgumentException(type.getCanonicalName() + " does not contain a valid factory method.");
  }

}
