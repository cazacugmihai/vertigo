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
package net.kuujo.vertigo.cluster.protocol;

import net.kuujo.vertigo.util.serializer.Serializer;
import net.kuujo.vertigo.util.serializer.SerializerFactory;

/**
 * A sync response.
 * 
 * @author Jordan Halterman
 */
public class SyncResponse extends Response {
  private static final Serializer serializer = SerializerFactory.getSerializer(SyncResponse.class);
  private long term;
  private boolean success;

  public SyncResponse() {
  }

  public SyncResponse(long term, boolean success) {
    this.term = term;
    this.success = success;
  }

  public static SyncResponse fromJson(String json) {
    return serializer.deserializeFromString(json, SyncResponse.class);
  }

  public static String toJson(SyncResponse response) {
    return serializer.serializeToString(response);
  }

  /**
   * Returns the requesting node's current term.
   * 
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the sync was successful.
   * 
   * @return Indicates whether the sync was successful.
   */
  public boolean success() {
    return success;
  }

}
