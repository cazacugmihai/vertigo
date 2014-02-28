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
package net.kuujo.vertigo.cluster.state;

import java.util.Iterator;

import net.kuujo.vertigo.cluster.config.ClusterConfig;
import net.kuujo.vertigo.cluster.log.CommandEntry;
import net.kuujo.vertigo.cluster.log.ConfigurationEntry;
import net.kuujo.vertigo.cluster.log.Entry;
import net.kuujo.vertigo.cluster.log.Entry.Type;
import net.kuujo.vertigo.cluster.log.Log;
import net.kuujo.vertigo.cluster.protocol.PingRequest;
import net.kuujo.vertigo.cluster.protocol.PollRequest;
import net.kuujo.vertigo.cluster.protocol.SubmitRequest;
import net.kuujo.vertigo.cluster.protocol.SyncRequest;
import net.kuujo.vertigo.cluster.state.impl.StateClient;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;

/**
 * An abstract state implementation.
 * 
 * @author Jordan Halterman
 */
abstract class State {
  protected Vertx vertx;
  protected StateClient stateClient;
  protected StateMachineExecutor stateMachine;
  protected Log log;
  protected ClusterConfig config;
  protected StateContext context;

  /**
   * Sets the vertx instance.
   * 
   * @param vertx A vertx instance.
   * @return The state instance.
   */
  public State setVertx(Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  /**
   * Sets the client.
   * 
   * @param stateClient A client instance.
   * @return The state instance.
   */
  public State setClient(StateClient stateClient) {
    this.stateClient = stateClient;
    return this;
  }

  /**
   * Sets the state machine.
   * 
   * @param stateMachine The state machine.
   * @return The state instance.
   */
  public State setStateMachine(StateMachineExecutor stateMachine) {
    this.stateMachine = stateMachine;
    return this;
  }

  /**
   * Sets the log.
   * 
   * @param log A log instance.
   * @return The state instance.
   */
  public State setLog(Log log) {
    this.log = log;
    return this;
  }

  /**
   * Sets the cluster configuration.
   * 
   * @param cluster The cluster configuration.
   * @return The state instance.
   */
  public State setConfig(ClusterConfig config) {
    this.config = config;
    return this;
  }

  /**
   * Sets the state context.
   * 
   * @param context A state context.
   * @return The state instance.
   */
  public State setContext(StateContext context) {
    this.context = context;
    return this;
  }

  /**
   * Starts up the state.
   * 
   * @param doneHandler A handler to be called once the state is started up.
   */
  public abstract void startUp(Handler<Void> doneHandler);

  /**
   * Executes a ping request.
   * 
   * @param request The request to execute.
   */
  public abstract void ping(PingRequest request);

  /**
   * Executes a sync request.
   * 
   * @param request The request to execute.
   */
  public abstract void sync(SyncRequest request);

  /**
   * Executes a poll request.
   * 
   * @param request The request to execute.
   */
  public abstract void poll(PollRequest request);

  /**
   * Executes a submit command request.
   * 
   * @param request The request to execute.
   */
  public abstract void submit(SubmitRequest request);

  /**
   * Tears down the state.
   * 
   * @param doneHandler A handler to be called once the state is shut down.
   */
  public abstract void shutDown(Handler<Void> doneHandler);

  /**
   * Handles a sync request.
   */
  protected boolean doSync(final SyncRequest request) {
    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.currentTerm()) {
      return false;
    }
    // Otherwise, continue on to check the log consistency.
    else {
      return checkConsistency(request);
    }
  }

  /**
   * Checks log consistency for a sync request.
   */
  private boolean checkConsistency(final SyncRequest request) {
    // If a previous log index and term were provided then check to ensure
    // that they match this node's previous log index and term.
    if (request.prevLogIndex() > 0 && request.prevLogTerm() > 0) {
      return checkPreviousEntry(request);
    }
    // Otherwise, continue on to check the entry being appended.
    else {
      return appendEntries(request);
    }
  }

  /**
   * Checks that the given previous log entry of a sync request matches the
   * previous log entry of this node.
   */
  private boolean checkPreviousEntry(final SyncRequest request) {
    // Check whether the log contains an entry at prevLogIndex.
    if (log.containsEntry(request.prevLogIndex())) {
      Entry entry = log.getEntry(request.prevLogIndex());
      if (entry.term() != request.prevLogTerm()) {
        return false;
      }
      else {
        return appendEntries(request);
      }
    }
    else {
      return false;
    }
  }

  /**
   * Appends request entries to the log.
   */
  private boolean appendEntries(final SyncRequest request) {
    return appendEntries(request.prevLogIndex(), request.entries().iterator(), request);
  }

  /**
   * Appends request entries to the log.
   */
  private boolean appendEntries(final long prevIndex, final Iterator<Entry> iterator, final SyncRequest request) {
    if (iterator.hasNext()) {
      final long index = prevIndex+1;
      final Entry entry = iterator.next();

      Entry loaded = log.getEntry(index);
      if (loaded == null) {
        log.appendEntry(entry);
      }
      else if (loaded.term() != entry.term()) {
        log.removeAfter(index-1);
        log.appendEntry(entry);
      }
      return appendEntries(index, iterator, request);
    }
    else {
      return checkApplyCommits(request);
    }
  }

  /**
   * Checks for entries that have been committed and applies committed entries
   * to the local state machine.
   */
  private boolean checkApplyCommits(final SyncRequest request) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous command applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply
    // them.
    if (request.commit() > context.commitIndex() || context.commitIndex() > context.lastApplied()) {
      // Update the local commit index with min(request commit, last log // index)
      long lastIndex = log.lastIndex();
      context.commitIndex(Math.min(request.commit(), lastIndex));

      // If the updated commit index indicates that commits remain to be
      // applied to the state machine, iterate entries and apply them.
      if (context.commitIndex() > Math.min(context.lastApplied(), lastIndex)) {
        return recursiveApplyCommits(context.lastApplied() + 1, Math.min(context.commitIndex(), lastIndex), request);
      }
      else {
        return true;
      }
    }
    // Otherwise, check whether the current term needs to be updated and reply
    // true to the sync request.
    else {
      return true;
    }
  }

  /**
   * Iteratively applies commits to the local state machine.
   */
  private boolean recursiveApplyCommits(final long index, final long ceiling, final SyncRequest request) {
    if (index <= ceiling) {
      // Load the log entry to be committed to the state machine.
      Entry entry = log.getEntry(index);
      if (entry == null) {
        return true;
      }

      if (entry.type().equals(Type.COMMAND)) {
        CommandEntry command = (CommandEntry) entry;
        try {
          stateMachine.applyCommand(command.command(), command.args());
        }
        catch (Exception e) {
        }
      }

      // If this is a configuration entry, update cluster membership. Since the
      // configuration was replicated to this node, it contains the *combined*
      // cluster membership during two-phase cluster configuration changes, so it's
      // safe to simply override the current cluster configuration.
      else if (entry.type().equals(Type.CONFIGURATION)) {
        context.members(((ConfigurationEntry) entry).members());
      }

      // Continue on to apply the next commit.
      return recursiveApplyCommits(index + 1, ceiling, request);
    }
    else {
      return true;
    }
  }

  /**
   * Handles a poll request.
   */
  protected boolean doPoll(final PollRequest request) {
    // If the requesting candidate is the current node then vote for self.
    if (request.candidate().equals(context.address())) {
      if (request.term() > context.currentTerm()) {
        context.currentTerm(request.term());
      }
      context.votedFor(request.candidate());
      return true;
    }

    // If the requesting candidate is not a known member of the cluster (to
    // this replica) then reject the vote. This helps ensure that new cluster
    // members cannot become leader until at least a majority of the cluster
    // has been notified of their membership.
    else if (!context.members().contains(request.candidate())) {
      return false;
    }

    // If the request term is greater than the current term then update
    // the local current term. This will also cause the candidate voted
    // for to be reset for the new term.
    else if (request.term() > context.currentTerm()) {
      context.currentTerm(request.term());
    }

    // If the request term is less than the current term then don't
    // vote for the candidate.
    if (request.term() < context.currentTerm()) {
      return false;
    }
    // If we haven't yet voted or already voted for this candidate then check
    // that the candidate's log is at least as up-to-date as the local log.
    else if (context.votedFor() == null || context.votedFor().equals(request.candidate())) {
      // It's possible that the last log index could be 0, indicating that
      // the log does not contain any entries. If that is the cases then
      // the log must *always* be at least as up-to-date as all other
      // logs.
      final long lastIndex = log.lastIndex();
      if (lastIndex == 0) {
        context.votedFor(request.candidate());
        return true;
      }
      else {
        // Load the log entry to get the term. We load the log entry
        // rather
        // than the log term to ensure that we're receiving the term from
        // the same entry as the loaded last log index.
        final Entry entry = log.getEntry(lastIndex);
        final long lastTerm = entry.term();
        if (request.lastLogIndex() >= lastIndex && request.lastLogTerm() >= lastTerm) {
          context.votedFor(request.candidate());
          return true;
        }
        else {
          context.votedFor(null); // Reset voted for.
          return false;
        }
      }
    }
    // If we've already voted for someone else then don't vote for the
    // candidate.
    else {
      return false;
    }
  }

}
