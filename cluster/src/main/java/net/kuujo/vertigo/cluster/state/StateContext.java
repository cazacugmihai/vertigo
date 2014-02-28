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

import java.util.HashSet;
import java.util.Set;

import net.kuujo.vertigo.cluster.ClusterException;
import net.kuujo.vertigo.cluster.config.ClusterConfig;
import net.kuujo.vertigo.cluster.log.Log;
import net.kuujo.vertigo.cluster.log.SnapshotEntry;
import net.kuujo.vertigo.cluster.log.impl.MemoryLog;
import net.kuujo.vertigo.cluster.protocol.PingRequest;
import net.kuujo.vertigo.cluster.protocol.PollRequest;
import net.kuujo.vertigo.cluster.protocol.SubmitRequest;
import net.kuujo.vertigo.cluster.protocol.SubmitResponse;
import net.kuujo.vertigo.cluster.protocol.SyncRequest;
import net.kuujo.vertigo.cluster.state.impl.StateClient;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A node state context.
 * 
 * @author Jordan Halterman
 */
public class StateContext {
  private static final Logger logger = LoggerFactory.getLogger(StateContext.class);
  private final String address;
  private final Vertx vertx;
  private final StateClient stateClient;
  private Log log;
  private StateMachineExecutor stateMachine;
  private ClusterConfig cluster = new ClusterConfig();
  private Set<String> members = new HashSet<>();
  private final StateFactory stateFactory = new StateFactory();
  private StateType stateType;
  private State state;
  private Handler<AsyncResult<String>> startHandler;
  private Handler<StateType> transitionHandler;
  private long electionTimeout = 5000;
  private long heartbeatInterval = 2500;
  private boolean useAdaptiveTimeouts = true;
  private double adaptiveTimeoutThreshold = 10;
  private boolean requireWriteMajority = true;
  private boolean requireReadMajority = true;
  private String currentLeader;
  private long currentTerm;
  private String votedFor;
  private long commitIndex = 0;
  private long lastApplied = 0;

  public StateContext(String address, Vertx vertx, StateMachineExecutor stateMachine) {
    this.address = address;
    this.vertx = vertx;
    this.stateClient = new StateClient(address, vertx);
    this.log = new MemoryLog();
    this.stateMachine = stateMachine;
    transition(StateType.START);
  }

  /**
   * Sets the state address.
   *
   * @param address The state address.
   * @return The state context.
   */
  public StateContext address(String address) {
    stateClient.setAddress(address);
    return this;
  }

  /**
   * Configures the state context.
   * 
   * @param config A cluster configuration.
   * @return The state context.
   */
  public StateContext configure(ClusterConfig config) {
    config.addMember(address);
    this.cluster = config;
    members = config.getMembers();
    if (state != null) {
      state.setConfig(config);
    }
    return this;
  }

  /**
   * Returns the cluster configuration.
   *
   * @return The cluster configuration.
   */
  public ClusterConfig config() {
    return cluster;
  }

  /**
   * Returns a set of cluster members.
   *
   * @return A set of cluster members.
   */
  public Set<String> members() {
    return members;
  }

  /**
   * Sets the cluster members.
   *
   * @param members A set of cluster members.
   * @return The state context.
   */
  public StateContext members(Set<String> members) {
    this.members = members;
    return this;
  }

  /**
   * Registers a state transition handler.
   *
   * @param handler A state transition handler.
   * @return The state context.
   */
  public StateContext transitionHandler(Handler<StateType> handler) {
    transitionHandler = handler;
    return this;
  }

  /**
   * Transitions to a new state.
   * 
   * @param type The new state type.
   * @return The state context.
   */
  public StateContext transition(final StateType type) {
    if (type.equals(stateType))
      return this;
    logger.info(address() + " transitioning to " + type.getName());
    final State oldState = state;
    stateType = type;
    switch (type) {
      case START:
        currentLeader = null;
        state = stateFactory.createStart()
            .setVertx(vertx)
            .setClient(stateClient)
            .setStateMachine(stateMachine)
            .setLog(log)
            .setConfig(cluster)
            .setContext(this);
        break;
      case FOLLOWER:
        state = stateFactory.createFollower()
            .setVertx(vertx)
            .setClient(stateClient)
            .setStateMachine(stateMachine)
            .setLog(log)
            .setConfig(cluster)
            .setContext(this);
        break;
      case CANDIDATE:
        state = stateFactory.createCandidate()
            .setVertx(vertx)
            .setClient(stateClient)
            .setStateMachine(stateMachine)
            .setLog(log)
            .setConfig(cluster)
            .setContext(this);
        break;
      case LEADER:
        state = stateFactory.createLeader()
            .setVertx(vertx)
            .setClient(stateClient)
            .setStateMachine(stateMachine)
            .setLog(log)
            .setConfig(cluster)
            .setContext(this);
        break;
    }

    final State newState = state;

    if (oldState != null) {
      oldState.shutDown(new Handler<Void>() {
        @Override
        public void handle(Void _) {
          unregisterHandlers();
          newState.startUp(new Handler<Void>() {
            @Override
            public void handle(Void _) {
              registerHandlers(newState);
              checkStart();
              if (transitionHandler != null) {
                transitionHandler.handle(type);
              }
            }
          });
        }
      });
    }
    else {
      newState.startUp(new Handler<Void>() {
        @Override
        public void handle(Void _) {
          registerHandlers(newState);
          checkStart();
          if (transitionHandler != null) {
            transitionHandler.handle(type);
          }
        }
      });
    }
    return this;
  }

  /**
   * Registers client handlers.
   */
  private void registerHandlers(final State state) {
    stateClient.pingHandler(new Handler<PingRequest>() {
      @Override
      public void handle(PingRequest request) {
        state.ping(request);
      }
    });
    stateClient.syncHandler(new Handler<SyncRequest>() {
      @Override
      public void handle(SyncRequest request) {
        state.sync(request);
      }
    });
    stateClient.pollHandler(new Handler<PollRequest>() {
      @Override
      public void handle(PollRequest request) {
        state.poll(request);
      }
    });
    stateClient.submitHandler(new Handler<SubmitRequest>() {
      @Override
      public void handle(SubmitRequest request) {
        state.submit(request);
      }
    });
  }

  /**
   * Unregisters client handlers.
   */
  private void unregisterHandlers() {
    stateClient.pingHandler(null);
    stateClient.syncHandler(null);
    stateClient.pollHandler(null);
    stateClient.submitHandler(null);
  }

  /**
   * Starts the context.
   *
   * @return
   *   The state context.
   */
  public StateContext start() {
    transition(StateType.START);
    stateClient.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.succeeded()) {
          setLogHandlers();
          transition(StateType.FOLLOWER);
        }
      }
    });
    return this;
  }

  /**
   * Starts the context.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the context is started.
   * @return
   *   The state context.
   */
  public StateContext start(final Handler<AsyncResult<String>> doneHandler) {
    transition(StateType.START);
    stateClient.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
        }
        else {
          startHandler = doneHandler;
          setLogHandlers();
          transition(StateType.FOLLOWER);
        }
      }
    });
    return this;
  }

  /**
   * Sets log handlers.
   */
  private void setLogHandlers() {
    log.fullHandler(new Handler<Void>() {
      @Override
      public void handle(Void _) {
        final long lastApplied = lastApplied();
        logger.info("Building snapshot");
        JsonObject snapshot = stateMachine.takeSnapshot();
        log.removeBefore(lastApplied+1);
        log.appendEntry(new SnapshotEntry(currentTerm(), snapshot));
      }
    });
  }

  /**
   * Checks whether the start handler needs to be called.
   */
  private void checkStart() {
    if (currentLeader != null && startHandler != null) {
      new DefaultFutureResult<String>(currentLeader).setHandler(startHandler);
      startHandler = null;
    }
  }

  /**
   * Stops the context.
   */
  public void stop() {
    stateClient.stop();
    log.close();
    transition(StateType.START);
  }

  /**
   * Stops the context.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the context is stopped.
   */
  public void stop(final Handler<AsyncResult<Void>> doneHandler) {
    stateClient.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        log.close();
        transition(StateType.START);
        new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
      }
    });
  }

  /**
   * Returns the current state type.
   *
   * @return The current state type.
   */
  public StateType currentState() {
    return stateType;
  }

  /**
   * Returns the endpoint address.
   * 
   * @return The current endpoint address.
   */
  public String address() {
    return address;
  }

  /**
   * Returns the endpoint to which the state belongs.
   * 
   * @return The parent endpoint.
   */
  StateClient stateClient() {
    return stateClient;
  }

  /**
   * Returns the state log.
   * 
   * @return The state log.
   */
  public Log log() {
    return log;
  }

  /**
   * Returns the replica election timeout.
   * 
   * @return The replica election timeout.
   */
  public long electionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the leader election timeout.
   * 
   * @param timeout The leader election timeout.
   * @return The state context.
   */
  public StateContext electionTimeout(long timeout) {
    electionTimeout = timeout;
    return this;
  }

  /**
   * Returns the replica heartbeat interval.
   * 
   * @return The replica heartbeat interval.
   */
  public long heartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the replica heartbeat interval.
   * 
   * @param interval The replica heartbeat interval.
   * @return The state context.
   */
  public StateContext heartbeatInterval(long interval) {
    heartbeatInterval = interval;
    return this;
  }

  /**
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   * 
   * @return Indicates whether adaptive timeouts are enabled.
   */
  public boolean useAdaptiveTimeouts() {
    return useAdaptiveTimeouts;
  }

  /**
   * Indicates whether the replica should use adaptive timeouts.
   * 
   * @param useAdaptive Indicates whether to use adaptive timeouts.
   * @return The state context.
   */
  public StateContext useAdaptiveTimeouts(boolean useAdaptive) {
    useAdaptiveTimeouts = useAdaptive;
    return this;
  }

  /**
   * Returns the adaptive timeout threshold.
   * 
   * @return The adaptive timeout threshold.
   */
  public double adaptiveTimeoutThreshold() {
    return adaptiveTimeoutThreshold;
  }

  /**
   * Sets the adaptive timeout threshold.
   * 
   * @param threshold The adaptive timeout threshold.
   * @return The state context.
   */
  public StateContext adaptiveTimeoutThreshold(double threshold) {
    adaptiveTimeoutThreshold = threshold;
    return this;
  }

  /**
   * Returns a boolean indicating whether majority replication is required for
   * write operations.
   * 
   * @return Indicates whether majority replication is required for write
   *         operations.
   */
  public boolean requireWriteMajority() {
    return requireWriteMajority;
  }

  /**
   * Sets whether majority replication is required for write operations.
   * 
   * @param require Indicates whether majority replication should be required
   *          for writes.
   * @return The state context.
   */
  public StateContext requireWriteMajority(boolean require) {
    requireWriteMajority = require;
    return this;
  }

  /**
   * Returns a boolean indicating whether majority synchronization is required
   * for read operations.
   * 
   * @return Indicates whether majority synchronization is required for read
   *         operations.
   */
  public boolean requireReadMajority() {
    return requireReadMajority;
  }

  /**
   * Sets whether majority synchronization is required for read operations.
   * 
   * @param require Indicates whether majority synchronization should be
   *          required for read operations.
   * @return The state context.
   */
  public StateContext requireReadMajority(boolean require) {
    requireReadMajority = require;
    return this;
  }

  /**
   * Returns the current leader.
   * 
   * @return The current leader.
   */
  public String currentLeader() {
    return currentLeader;
  }

  /**
   * Sets the current leader.
   * 
   * @param address The current leader.
   * @return The state context.
   */
  public StateContext currentLeader(String address) {
    if (currentLeader == null || !currentLeader.equals(address)) {
      logger.debug(String.format("Current cluster leader changed: %s", address));
    }
    currentLeader = address;
    checkStart();
    return this;
  }

  /**
   * Returns the current term.
   * 
   * @return The current term.
   */
  public long currentTerm() {
    return currentTerm;
  }

  /**
   * Sets the current term.
   * 
   * @param term The current term.
   * @return The state context.
   */
  public StateContext currentTerm(long term) {
    if (term > currentTerm) {
      currentTerm = term;
      logger.debug(String.format("Updated current term %d", term));
      votedFor = null;
    }
    return this;
  }

  /**
   * Returns the address of the member last voted for.
   * 
   * @return The address of the member last voted for.
   */
  public String votedFor() {
    return votedFor;
  }

  /**
   * Sets the address of the member last voted for.
   * 
   * @param address The address of the member last voted for.
   * @return The state context.
   */
  public StateContext votedFor(String address) {
    if (votedFor == null || !votedFor.equals(address)) {
      logger.debug(String.format("Voted for %s", address));
    }
    votedFor = address;
    return this;
  }

  /**
   * Returns the current commit index.
   * 
   * @return The current commit index.
   */
  public long commitIndex() {
    return commitIndex;
  }

  /**
   * Sets the current commit index.
   * 
   * @param index The current commit index.
   * @return The state context.
   */
  public StateContext commitIndex(long index) {
    commitIndex = index;
    return this;
  }

  /**
   * Returns the last index applied to the state machine.
   * 
   * @return The last index applied to the state machine.
   */
  public long lastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last index applied to the state machine.
   * 
   * @param index The last index applied to the state machine.
   * @return The state context.
   */
  public StateContext lastApplied(long index) {
    lastApplied = index;
    return this;
  }

  /**
   * Submits a command to the context.
   *
   * @param command The command to submit.
   * @param args The command arguments.
   * @param doneHandler An asynchronous handler to be called with the command result.
   * @return The state context.
   */
  public <R> StateContext submitCommand(final String command, final JsonObject args, final Handler<AsyncResult<R>> doneHandler) {
    if (currentLeader != null) {
      stateClient.submit(currentLeader, new SubmitRequest(command, args), new Handler<AsyncResult<SubmitResponse>>() {
        @Override
        @SuppressWarnings("unchecked")
        public void handle(AsyncResult<SubmitResponse> result) {
          if (result.failed()) {
            new DefaultFutureResult<R>(result.cause()).setHandler(doneHandler);
          }
          else {
            new DefaultFutureResult<R>((R) result.result().result()).setHandler(doneHandler);
          }
        }
      });
    }
    else {
      new DefaultFutureResult<R>(new ClusterException("No leader available.")).setHandler(doneHandler);
    }
    return this;
  }

}
