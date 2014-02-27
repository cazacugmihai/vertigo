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
package net.kuujo.vertigo.cluster.log;

import java.util.Map;

import org.vertx.java.core.json.JsonObject;

/**
 * A special snapshot log entry.
 * 
 * @author Jordan Halterman
 */
public class SnapshotEntry extends Entry {
  private Map<String, Object> snapshot;

  public SnapshotEntry() {
    super();
  }

  public SnapshotEntry(long term, Map<String, Object> snapshot) {
    super(term);
    this.snapshot = snapshot;
  }

  public SnapshotEntry(long term, JsonObject snapshot) {
    super(term);
    this.snapshot = snapshot.toMap();
  }

  @Override
  public Type type() {
    return Type.CONFIGURATION;
  }

  /**
   * Returns a set of updated cluster members.
   * 
   * @return A set of cluster member addresses.
   */
  public JsonObject snapshot() {
    return new JsonObject(snapshot);
  }

}
