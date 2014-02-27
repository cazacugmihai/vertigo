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
package net.kuujo.vertigo.cluster.impl;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.kuujo.vertigo.cluster.DeploymentInfo;
import net.kuujo.vertigo.cluster.VerticleDeploymentInfo;

/**
 * Default verticle deployment info implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultVerticleDeploymentInfo extends AbstractDeploymentInfo implements VerticleDeploymentInfo {
  private String main;
  private boolean worker;
  @JsonProperty("multi-threaded")
  private boolean multiThreaded;

  @Override
  public Type type() {
    return DeploymentInfo.Type.VERTICLE;
  }

  @JsonGetter("type")
  private String getDeploymentTypeName() {
    return DeploymentInfo.Type.VERTICLE.getName();
  }

  @Override
  public boolean isModule() {
    return false;
  }

  @Override
  public boolean isVerticle() {
    return true;
  }

  @Override
  public String main() {
    return main;
  }

  @Override
  public boolean isWorker() {
    return worker;
  }

  @Override
  public boolean isMultiThreaded() {
    return multiThreaded;
  }

}
