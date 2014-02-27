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
package net.kuujo.vertigo.context.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.kuujo.vertigo.context.ComponentContext;
import net.kuujo.vertigo.context.ConnectionContext;
import net.kuujo.vertigo.context.InputContext;
import net.kuujo.vertigo.context.InputStreamContext;
import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.context.ModuleContext;
import net.kuujo.vertigo.context.NetworkContext;
import net.kuujo.vertigo.context.OutputContext;
import net.kuujo.vertigo.context.OutputStreamContext;
import net.kuujo.vertigo.context.VerticleContext;
import net.kuujo.vertigo.network.Component;
import net.kuujo.vertigo.network.Input;
import net.kuujo.vertigo.network.MalformedNetworkException;
import net.kuujo.vertigo.network.Module;
import net.kuujo.vertigo.network.Network;
import net.kuujo.vertigo.network.Verticle;

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
  public static NetworkContext buildContext(Network network) {
    NetworkContext.Builder context = NetworkContext.Builder.newBuilder();

    // Set basic network configuration options.
    context.setAddress(network.getAddress());
    context.setAckingEnabled(network.isAckingEnabled());
    context.setMessageTimeout(network.getMessageTimeout());

    // Set up network auditors with unique addresses.
    Set<String> auditors = new HashSet<>();
    for (int i = 1; i <= network.getNumAuditors(); i++) {
      auditors.add(String.format("%s.auditor.%d", network.getAddress(), i));
    }
    context.setAuditors(auditors);

    // Set up network components without inputs. Inputs are stored in a map so
    // that they can be set up after all component instances have been set up.
    Map<String, ComponentContext<?>> components = new HashMap<>();
    Map<String, List<Input>> inputs = new HashMap<>();
    for (Component<?> component : network.getComponents()) {
      // Store the component inputs for later setup.
      inputs.put(component.getAddress(), component.getInputs());

      if (component.isModule()) {
        // Set up basic module configuratin options.
        ModuleContext.Builder module = ModuleContext.Builder.newBuilder();
        module.setAddress(component.getAddress());
        module.setType(component.getType());
        module.setModule(((Module) component).getModule());
        module.setConfig(component.getConfig());
        module.setHooks(component.getHooks());
        module.setDeploymentTargets(component.getDeploymentTargets());

        // Set up module instances.
        List<InstanceContext> instances = new ArrayList<>();
        for (int i = 1; i <= component.getNumInstances(); i++) {
          InstanceContext.Builder instance = InstanceContext.Builder.newBuilder();
          instance.setId(String.format("%s-%d", component.getAddress(), i));
          instance.setNumber(i);
          instance.setInput(InputContext.Builder.newBuilder().build());
          instance.setOutput(OutputContext.Builder.newBuilder().build());
          instances.add(instance.build());
        }
        module.setInstances(instances);

        components.put(component.getAddress(), module.build());
      }
      else {
        // Set up basic verticle configuration options.
        VerticleContext.Builder verticle = VerticleContext.Builder.newBuilder();
        verticle.setAddress(component.getAddress());
        verticle.setType(component.getType());
        verticle.setMain(((Verticle) component).getMain());
        verticle.setWorker(((Verticle) component).isWorker());
        verticle.setMultiThreaded(((Verticle) component).isMultiThreaded());
        verticle.setConfig(component.getConfig());
        verticle.setHooks(component.getHooks());
        verticle.setDeploymentTargets(component.getDeploymentTargets());

        // Set up module instances.
        List<InstanceContext> instances = new ArrayList<>();
        for (int i = 1; i <= component.getNumInstances(); i++) {
          InstanceContext.Builder instance = InstanceContext.Builder.newBuilder();
          instance.setId(String.format("%s-%d", component.getAddress(), i));
          instance.setNumber(i);
          instance.setInput(InputContext.Builder.newBuilder().build());
          instance.setOutput(OutputContext.Builder.newBuilder().build());
          instances.add(instance.build());
        }
        verticle.setInstances(instances);

        components.put(component.getAddress(), verticle.build());
      }
    }

    // Iterate through all inputs in the network and set up input/output streams.
    for (Map.Entry<String, List<Input>> entry : inputs.entrySet()) {
      List<Input> inputInfo = entry.getValue();
      for (Input info : inputInfo) {

        // Set up the output stream. Each connection between components (represented
        // as an Input instance) will have an associated InputStreamContext and
        // OutputStreamContext. The OutputStreamContext contains a set of connections
        // to each instance to which the stream feeds. The InputStreamContext will
        // contain a single connection on which the instance listens for messages.
        OutputStreamContext.Builder outputStream = OutputStreamContext.Builder.newBuilder();
        outputStream.setStream(info.getStream());
        outputStream.setGrouping(info.getGrouping());

        // The input component is the entry key.
        ComponentContext<?> inputComponentContext = components.get(entry.getKey());

        // The output component is referenced by the Input instance.
        ComponentContext<?> outputComponentContext = components.get(info.getAddress());

        if (inputComponentContext != null && outputComponentContext != null) {
          // Iterate through input instances and add unique addresses to the output stream.
          for (InstanceContext inputInstanceContext : inputComponentContext.instances()) {
            InputStreamContext.Builder inputStream = InputStreamContext.Builder.newBuilder();
            inputStream.setStream(info.getStream());
            ConnectionContext connection = ConnectionContext.Builder.newBuilder().setAddress(UUID.randomUUID().toString()).build();
            inputStream.setConnection(connection); // Set the input stream connection.
            outputStream.addConnection(connection); // Add the connection to the output stream.
            InputContext.Builder.newBuilder(inputInstanceContext.input()).addStream(inputStream.build()).build();
          }

          // Iterate through output instances and add the output stream.
          for (InstanceContext outputInstanceContext : outputComponentContext.instances()) {
            OutputContext.Builder.newBuilder(outputInstanceContext.output()).addStream(outputStream.build()).build();
          }
        }
      }
    }

    // Set the components on the network context and build the final context.
    context.setComponents(components.values());
    return context.build();
  }

}
