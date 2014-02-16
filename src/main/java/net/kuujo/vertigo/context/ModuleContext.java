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

/**
 * Module component context.
 *
 * @author Jordan Halterman
 *
 * @param <T> The component type
 */
public class ModuleContext extends ComponentContext<ModuleContext> {
  private String module;

  @Override
  protected String getDeploymentType() {
    return Component.COMPONENT_DEPLOYMENT_MODULE;
  }

  @Override
  public boolean isModule() {
    return true;
  }

  /**
   * Returns the module name.
   *
   * @return
   *   The module name.
   */
  public String module() {
    return module;
  }

  public String moduleName() {
    return module();
  }

}
