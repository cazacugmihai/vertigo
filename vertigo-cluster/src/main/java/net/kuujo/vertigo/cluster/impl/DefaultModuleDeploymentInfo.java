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

import net.kuujo.vertigo.cluster.DeploymentInfo;
import net.kuujo.vertigo.cluster.ModuleDeploymentInfo;

/**
 * Default module deployment info implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultModuleDeploymentInfo extends AbstractDeploymentInfo implements ModuleDeploymentInfo {
  private String module;

  @Override
  public Type type() {
    return DeploymentInfo.Type.MODULE;
  }

  @JsonGetter("type")
  private String getDeploymentTypeName() {
    return DeploymentInfo.Type.MODULE.getName();
  }

  @Override
  public boolean isModule() {
    return true;
  }

  @Override
  public boolean isVerticle() {
    return false;
  }

  @Override
  public String module() {
    return module;
  }

}
