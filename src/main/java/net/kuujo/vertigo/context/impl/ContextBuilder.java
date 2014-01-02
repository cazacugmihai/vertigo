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
package net.kuujo.vertigo.context.impl;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.vertigo.context.NetworkContext;
import net.kuujo.vertigo.network.Component;
import net.kuujo.vertigo.network.Config;
import net.kuujo.vertigo.network.MalformedNetworkException;
import net.kuujo.vertigo.network.Network;
import net.kuujo.vertigo.serializer.SerializationException;
import net.kuujo.vertigo.serializer.Serializer;
import net.kuujo.vertigo.serializer.SerializerFactory;
import net.kuujo.vertigo.util.Address;
import net.kuujo.vertigo.util.Identifier;

/**
 * A context builder.
 *
 * @author Jordan Halterman
 */
public final class ContextBuilder {

  /**
   * Builds a network context from a network definition.
   *
   * @param network
   *   The network definition.
   * @return
   *   A new network context.
   * @throws MalformedNetworkException 
   *   If the network is malformed.
   */
  @SuppressWarnings("deprecation")
  public static NetworkContext buildContext(Network network) throws MalformedNetworkException {
    try {
      Serializer<Network> serializer = SerializerFactory.getSerializer(Network.class);
      JsonObject serialized = serializer.serialize(network);
      serialized.putString("id", Identifier.formatNetworkId(network));
      serialized.putString("address", Address.formatNetworkAddress(network));

      JsonObject config = serialized.getObject(Network.NETWORK_CONFIG);
      JsonObject networkConfig = config.getObject(Config.NETWORK);
      serialized.putBoolean(Config.NETWORK_ACKING_ENABLED, networkConfig.getBoolean(Config.NETWORK_ACKING_ENABLED));
      serialized.putNumber(Config.NETWORK_ACK_TIMEOUT, networkConfig.getLong(Config.NETWORK_ACK_TIMEOUT));

      JsonArray auditors = new JsonArray();
      for (int i = 1; i < network.getNetworkConfig().getNetworkNumAuditors()+1; i++) {
        auditors.add(String.format("%s.auditor.%d", network.getAddress(), i));
      }
      serialized.putArray("auditors", auditors);

      JsonObject jsonComponents = serialized.getObject(Network.NETWORK_COMPONENTS);
      for (Component<?> component : network.getComponents()) {
        JsonObject jsonComponent = jsonComponents.getObject(component.getAddress());
        jsonComponent.putString("id", Identifier.formatComponentId(component));
        jsonComponent.putString("address", Address.formatComponentAddress(component));

        JsonArray instances = new JsonArray();
        for (int i = 1; i <= component.getNumInstances(); i++) {
          instances.add(new JsonObject().putString("id", Identifier.formatInstanceId(component, i))
              .putString("address", Address.formatInstanceAddress(component, i)));
        }
        jsonComponent.putArray("instances", instances);
      }
      return SerializerFactory.getSerializer(NetworkContext.class).deserialize(serialized);
    }
    catch (SerializationException e) {
      throw new MalformedNetworkException(e);
    }
  }

}
