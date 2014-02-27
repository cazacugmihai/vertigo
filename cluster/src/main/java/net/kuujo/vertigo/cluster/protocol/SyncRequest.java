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

import java.util.List;

import net.kuujo.vertigo.cluster.log.Entry;
import net.kuujo.vertigo.util.serializer.Serializer;
import net.kuujo.vertigo.util.serializer.SerializerFactory;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * A sync request.
 * 
 * @author Jordan Halterman
 */
public class SyncRequest extends Request {
  private static final Serializer serializer = SerializerFactory.getSerializer(SyncRequest.class);
  private long term;
  private String leader;
  private long prevLogIndex;
  private long prevLogTerm;
  private List<Entry> entries;
  private long commit;

  public SyncRequest() {
  }

  public SyncRequest(long term, String leader, long prevLogIndex, long prevLogTerm, List<Entry> entries, long commitIndex) {
    this.term = term;
    this.leader = leader;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.entries = entries;
    this.commit = commitIndex;
  }

  public static SyncRequest fromJson(String json) {
    return serializer.deserializeFromString(json, SyncRequest.class);
  }

  public static SyncRequest fromJson(String json, Message<?> message) {
    return serializer.deserializeFromString(json, SyncRequest.class).setMessage(message);
  }

  public static String toJson(SyncRequest request) {
    return serializer.serializeToString(request);
  }

  private SyncRequest setMessage(Message<?> message) {
    this.message = message;
    return this;
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
   * Returns the requesting leader address.
   * 
   * @return The leader's address.
   */
  public String leader() {
    return leader;
  }

  /**
   * Returns the index of the log entry preceding the new entry.
   * 
   * @return The index of the log entry preceding the new entry.
   */
  public long prevLogIndex() {
    return prevLogIndex;
  }

  /**
   * Returns the term of the log entry preceding the new entry.
   * 
   * @return The index of the term preceding the new entry.
   */
  public long prevLogTerm() {
    return prevLogTerm;
  }

  /**
   * Returns the log entries to append.
   * 
   * @return A list of log entries.
   */
  public List<Entry> entries() {
    return entries;
  }

  /**
   * Returns the leader's commit index.
   * 
   * @return The leader commit index.
   */
  public long commit() {
    return commit;
  }

  /**
   * Replies to the request.
   * 
   * @param term The responding node's current term.
   * @param success Indicates whether the sync was successful.
   */
  public void reply(long term, boolean success) {
    reply(new JsonObject().putNumber("term", term).putBoolean("success", success));
  }

}
