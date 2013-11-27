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
package net.kuujo.vertigo.network;

/**
 * A module component.
 *
 * @author Jordan Halterman
 */
public class Module extends Component<Module> {
  private String module;

  public Module() {
  }

  public Module(String address) {
    super(address);
  }

  @Override
  public String getType() {
    return MODULE;
  }

  /**
   * Returns the module name.
   *
   * @return
   *   The module name.
   */
  public String getModule() {
    return module;
  }

  /**
   * Sets the module name.
   *
   * @param moduleName
   *   The module name.
   * @return
   *   The called module component.
   */
  public Module setModule(String moduleName) {
    this.module = moduleName;
    return this;
  }

}
