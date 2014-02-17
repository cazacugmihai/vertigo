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
package net.kuujo.vertigo.context;

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

}
