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
package net.kuujo.vertigo.util;

/**
 * Instance utility methods.
 *
 * @author Jordan Halterman
 */
public final class Instance {

  /**
   * Formats an instance name.
   *
   * @param component
   *   The component to which the instance belongs.
   * @param instanceNumber
   *   The instance number.
   * @return
   *   The formatted instance name.
   */
  public static String formatInstanceId(net.kuujo.vertigo.network.Component<?> component, int instanceNumber) {
    if (component.isModule()) {
      return String.format(component.getNetwork().getNetworkConfig().getComponentIdFormat(),
          component.getNetwork().getAddress(), component.getNetwork().getNetworkId(),
          component.getAddress(), Component.serializeType(component.getType()),
          component.getModule(), component.getComponentId(), instanceNumber);
    }
    else if (component.isVerticle()) {
      return String.format(component.getNetwork().getNetworkConfig().getComponentIdFormat(),
          component.getNetwork().getAddress(), component.getNetwork().getNetworkId(),
          component.getAddress(), Component.serializeType(component.getType()),
          component.getMain(), component.getComponentId(), instanceNumber);
    }
    return null;
  }

}
