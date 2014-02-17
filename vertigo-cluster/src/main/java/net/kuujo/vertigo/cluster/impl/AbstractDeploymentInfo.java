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

import java.util.Map;
import java.util.Set;

import org.vertx.java.core.json.JsonObject;

import net.kuujo.vertigo.cluster.DeploymentInfo;

/**
 * An abstract deployment info implementation.
 * 
 * @author Jordan Halterman
 */
public abstract class AbstractDeploymentInfo implements DeploymentInfo {
  private String id;
  private Set<String> targets;
  private Map<String, Object> config;
  private int instances = 1;

  @Override
  public String id() {
    return id;
  }

  @Override
  public Set<String> targets() {
    return targets;
  }

  @Override
  public JsonObject config() {
    return new JsonObject(config);
  }

  @Override
  public int instances() {
    return instances;
  }

}
