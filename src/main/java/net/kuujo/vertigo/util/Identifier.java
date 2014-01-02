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

import net.kuujo.vertigo.network.Network;

/**
 * Network/component/instance identifier helpers.
 *
 * @author Jordan Halterman
 */
public final class Identifier {

  /**
   * Formats a network ID.
   *
   * @param network
   *   The network.
   * @return
   *   The formatted network ID.
   */
  public static String formatNetworkId(Network network) {
    return String.format(network.getNetworkConfig().getNetworkIdFormat(), network.getName());
  }

  /**
   * Formats a component ID.
   *
   * @param component
   *   The component.
   * @return
   *   The formatted component ID.
   */
  public static String formatComponentId(net.kuujo.vertigo.network.Component<?> component) {
    if (component.isModule()) {
      return String.format(component.getNetwork().getNetworkConfig().getComponentIdFormat(),
          component.getNetwork().getName(), Component.serializeType(component.getType()),
          component.getName(), component.getModule());
    }
    else if (component.isVerticle()) {
      return String.format(component.getNetwork().getNetworkConfig().getComponentIdFormat(),
          component.getNetwork().getName(), Component.serializeType(component.getType()),
          component.getName(), component.getMain());
    }
    return null;
  }

  /**
   * Formats an instance ID.
   *
   * @param component
   *   The component to which the instance belongs.
   * @param instanceNumber
   *   The instance number.
   * @return
   *   The formatted instance ID.
   */
  public static String formatInstanceId(net.kuujo.vertigo.network.Component<?> component, int instanceNumber) {
    if (component.isModule()) {
      return String.format(component.getNetwork().getNetworkConfig().getInstanceIdFormat(),
          component.getNetwork().getName(), Component.serializeType(component.getType()),
          component.getName(), component.getModule(), instanceNumber);
    }
    else if (component.isVerticle()) {
      return String.format(component.getNetwork().getNetworkConfig().getInstanceIdFormat(),
          component.getNetwork().getName(), Component.serializeType(component.getType()),
          component.getName(), component.getMain(), instanceNumber);
    }
    return null;
  }

}
