/*
 * Copyright 2013-2014 the original author or authors.
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
package net.kuujo.vertigo.output.selector;

import java.util.List;

import net.kuujo.vertigo.network.ConnectionConfig;
import net.kuujo.vertigo.output.OutputConnection;
import net.kuujo.vertigo.util.serializer.JsonSerializable;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * An output selector.
 *
 * Output selectors are the counterparts to input groupings. When an input is
 * used to subscribe to the output of another component, the input's grouping
 * is converted to an output {@link Selector}. Each time a message is emitted
 * to the resulting output channel, the selector is used to select which
 * {@link ConnectionConfig}s to which to send the message.
 *
 * @author Jordan Halterman
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="type")
@JsonSubTypes({
  @JsonSubTypes.Type(value=RandomSelector.class, name="random"),
  @JsonSubTypes.Type(value=RoundSelector.class, name="round"),
  @JsonSubTypes.Type(value=HashSelector.class, name="fields"),
  @JsonSubTypes.Type(value=FairSelector.class, name="fair"),
  @JsonSubTypes.Type(value=AllSelector.class, name="all")
})
public interface Selector extends JsonSerializable {

  /**
   * Selects a list of connections to which to send a message.
   *
   * @param message The message being sent.
   * @param targets A list of connections from which to select.
   * @return A list of selected connections.
   */
  List<OutputConnection> select(Object message, List<OutputConnection> connections);

}
