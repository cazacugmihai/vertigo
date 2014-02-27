/*
 * Copyright 2013-2014 the original author or authors.
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
package net.kuujo.vertigo.context;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vertx.java.core.json.JsonObject;

import net.kuujo.vertigo.hooks.ComponentHook;
import net.kuujo.vertigo.network.Component;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Verticle component context.
 * 
 * @author Jordan Halterman
 * 
 * @param <T> The component type
 */
public class VerticleContext extends ComponentContext<VerticleContext> {
  private String main;
  private boolean worker;
  @JsonProperty("multi-threaded")
  private boolean multiThreaded;

  @Override
  protected String getDeploymentType() {
    return Component.COMPONENT_DEPLOYMENT_VERTICLE;
  }

  @Override
  public boolean isVerticle() {
    return true;
  }

  /**
   * Returns the verticle main.
   * 
   * @return The verticle main.
   */
  public String main() {
    return main;
  }

  /**
   * Returns a boolean indicating whether the verticle is a worker verticle.
   * 
   * @return Indicates whether the verticle is a worker verticle.
   */
  public boolean isWorker() {
    return worker;
  }

  /**
   * Returns a boolean indicating whether the verticle is a worker and is multi-threaded.
   * If the verticle is not a worker then <code>false</code> will be returned.
   * 
   * @return Indicates whether the verticle is a worker and is multi-threaded.
   */
  public boolean isMultiThreaded() {
    return isWorker() && multiThreaded;
  }

  /**
   * Verticle context builder.
   *
   * @author Jordan Halterman
   */
  public static class Builder {
    private VerticleContext context;

    private Builder() {
      context = new VerticleContext();
    }

    private Builder(VerticleContext context) {
      this.context = context;
    }

    /**
     * Creates a new context builder.
     *
     * @return A new verticle context builder.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Creates a new context builder.
     *
     * @param context A starting verticle context.
     * @return A new verticle context builder.
     */
    public static Builder newBuilder(VerticleContext context) {
      return new Builder(context);
    }

    /**
     * Sets the component address.
     *
     * @param address The component event bus address.
     * @return The context builder.
     */
    public Builder setAddress(String address) {
      context.address = address;
      return this;
    }

    /**
     * Sets the component type.
     *
     * @param type A component type constant.
     * @return The context builder.
     */
    public Builder setType(Component.Type type) {
      context.type = type;
      return this;
    }

    /**
     * Sets the verticle main.
     *
     * @param main The verticle component main.
     * @return The context builder.
     */
    public Builder setMain(String main) {
      context.main = main;
      return this;
    }

    /**
     * Sets the component configuration.
     *
     * @param config The component configuration.
     * @return The context builder.
     */
    public Builder setConfig(JsonObject config) {
      context.config = config.toMap();
      return this;
    }

    /**
     * Sets the component configuration.
     *
     * @param config The component configuration.
     * @return The context builder.
     */
    public Builder setConfig(Map<String, Object> config) {
      context.config = config;
      return this;
    }

    /**
     * Sets whether the verticle is a worker.
     *
     * @param isWorker Whether the verticle is a worker.
     * @return The context builder.
     */
    public Builder setWorker(boolean isWorker) {
      context.worker = isWorker;
      return this;
    }

    /**
     * Sets whether the verticle is a multi-threaded worker.
     *
     * @param isMultiThreaded Whether the verticle is a multi-threaded worker.
     * @return The context builder.
     */
    public Builder setMultiThreaded(boolean isMultiThreaded) {
      if (isMultiThreaded) {
        context.worker = true;
      }
      context.multiThreaded = isMultiThreaded;
      return this;
    }

    /**
     * Sets the component deployment targets.
     *
     * @param targets An array of deployment targets.
     * @return The context builder.
     */
    public Builder setDeploymentTargets(String... targets) {
      context.targets = new HashSet<String>(Arrays.asList(targets));
      return this;
    }

    /**
     * Sets the component deployment targets.
     *
     * @param targets A set of component deployment targets.
     * @return The context builder.
     */
    public Builder setDeploymentTargets(Set<String> targets) {
      context.targets = targets;
      return this;
    }

    /**
     * Adds a component deployment target.
     *
     * @param target A deployment target to add.
     * @return The context builder.
     */
    public Builder addDeploymentTarget(String target) {
      context.targets.add(target);
      return this;
    }

    /**
     * Removes a component deployment target.
     *
     * @param target A deployment target to remove.
     * @return The context builder.
     */
    public Builder removeDeploymentTarget(String target) {
      context.targets.remove(target);
      return this;
    }

    /**
     * Sets component hooks.
     *
     * @param hooks An array of component hooks.
     * @return The context builder.
     */
    public Builder setHooks(ComponentHook... hooks) {
      context.hooks = Arrays.asList(hooks);
      return this;
    }

    /**
     * Sets the component hooks.
     *
     * @param hooks A list of component hooks.
     * @return The context builder.
     */
    public Builder setHooks(List<ComponentHook> hooks) {
      context.hooks = hooks;
      return this;
    }

    /**
     * Adds a hook to the component.
     *
     * @param hook A component hook instance.
     * @return The context builder.
     */
    public Builder addHook(ComponentHook hook) {
      context.hooks.add(hook);
      return this;
    }

    /**
     * Removes a hook from the component.
     *
     * @param hook A component hook instance.
     * @return The context builder.
     */
    public Builder removeHook(ComponentHook hook) {
      context.hooks.remove(hook);
      return this;
    }

    /**
     * Sets the component instance contexts.
     *
     * @param instances An array of instance contexts.
     * @return The context builder.
     */
    public Builder setInstances(InstanceContext... hooks) {
      context.instances = Arrays.asList(hooks);
      return this;
    }

    /**
     * Sets the component instance contexts.
     *
     * @param instances A list of instance contexts.
     * @return The context builder.
     */
    public Builder setInstances(List<InstanceContext> hooks) {
      context.instances = hooks;
      return this;
    }

    /**
     * Adds an instance context to the component.
     *
     * @param instance An instance context to add.
     * @return The context builder.
     */
    public Builder addInstance(InstanceContext hook) {
      context.instances.add(hook);
      return this;
    }

    /**
     * Removes an instance context from the component.
     *
     * @param instance An instance context to remove.
     * @return The context builder.
     */
    public Builder removeInstance(InstanceContext hook) {
      context.instances.remove(hook);
      return this;
    }

    /**
     * Builds the verticle context.
     *
     * @return A new verticle context.
     */
    public VerticleContext build() {
      return context;
    }
  }

}
