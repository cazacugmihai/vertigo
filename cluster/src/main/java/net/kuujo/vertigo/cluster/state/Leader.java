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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;

import net.kuujo.vertigo.cluster.annotations.Command;
import net.kuujo.vertigo.cluster.config.ClusterConfig;
import net.kuujo.vertigo.cluster.log.CommandEntry;
import net.kuujo.vertigo.cluster.log.ConfigurationEntry;
import net.kuujo.vertigo.cluster.log.Entry;
import net.kuujo.vertigo.cluster.log.NoOpEntry;
import net.kuujo.vertigo.cluster.protocol.PingRequest;
import net.kuujo.vertigo.cluster.protocol.PingResponse;
import net.kuujo.vertigo.cluster.protocol.PollRequest;
import net.kuujo.vertigo.cluster.protocol.SubmitRequest;
import net.kuujo.vertigo.cluster.protocol.SyncRequest;
import net.kuujo.vertigo.cluster.protocol.SyncResponse;
import net.kuujo.vertigo.cluster.state.impl.Majority;
import net.kuujo.vertigo.cluster.state.impl.StateLock;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A leader state.
 * 
 * @author Jordan Halterman
 */
class Leader extends State implements Observer {
  private static final int BATCH_SIZE = 10;
  private static final Logger logger = LoggerFactory.getLogger(Leader.class);
  private final StateLock configLock = new StateLock();
  private long pingTimer;
  private final Set<Long> periodicTimers = new HashSet<>();
  private List<Replica> replicas;
  private Map<String, Replica> replicaMap = new HashMap<>();
  private final Set<Majority> majorities = new HashSet<>();

  @Override
  public void startUp(final Handler<Void> doneHandler) {
    // Create a set of replica references in the cluster.
    members = config.getMembers();
    remoteMembers = new HashSet<>(members);
    remoteMembers.remove(context.address());
    replicas = new ArrayList<>();
    for (String address : remoteMembers) {
      Replica replica = new Replica(address);
      replicaMap.put(address, replica);
      replicas.add(replica);
    }

    // Set up a timer for pinging cluster members.
    pingTimer = vertx.setPeriodic(context.heartbeatInterval(), new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        for (Replica replica : replicas) {
          replica.update();
        }
      }
    });

    // Immediately commit a NOOP entry to the log. If the commit fails
    // then we periodically retry appending the entry until successful.
    // The leader cannot start until this no-op entry has been
    // successfully appended.
    log.appendEntry(new NoOpEntry(context.currentTerm()));

    // Once the no-op entry has been appended, immediately update
    // all nodes.
    for (Replica replica : replicas) {
      replica.update();
    }

    // Observe the cluster configuration for changes.
    config.addObserver(Leader.this);
    clusterChanged(config);
    context.currentLeader(context.address());
    doneHandler.handle((Void) null);
  }

  @Override
  public void update(Observable config, Object arg) {
    clusterChanged((ClusterConfig) config);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private void clusterChanged(final ClusterConfig config) {
    updateClusterConfig(config.getMembers());
  }

  /**
   * Updates cluster membership in a two-phase process.
   */
  private void updateClusterConfig(final Set<String> members) {
    // Use a lock to ensure that only one configuration change may take place
    // at any given time. This lock is separate from the global state lock.
    configLock.acquire(new Handler<Void>() {
      @Override
      public void handle(Void _) {

        // Create a set of combined cluster membership between the old configuration
        // and the new/updated configuration.
        final Set<String> combinedMembers = new HashSet<>(Leader.this.members);
        combinedMembers.addAll(members);

        // Append a new configuration entry to the log containing the combined
        // cluster membership.
        final long index = log.appendEntry(new ConfigurationEntry(context.currentTerm(), combinedMembers));

        // Replicate the combined configuration to a majority of the cluster.
        writeMajority(index, new Handler<Boolean>() {
          @Override
          public void handle(Boolean succeeded) {
            if (succeeded) {
              // Once the combined configuration has been replicated, apply the
              // configuration to the current state (internal state machine).
              Leader.this.members = combinedMembers;
              Leader.this.remoteMembers = new HashSet<>(combinedMembers);
              Leader.this.remoteMembers.remove(context.address());

              // Update replica references to reflect the configuration changes.
              for (String address : Leader.this.remoteMembers) {
                if (!replicaMap.containsKey(address)) {
                  Replica replica = new Replica(address);
                  replica.ping();
                  replicaMap.put(address, replica);
                  replicas.add(replica);
                }
              }

              // Now that the combined configuration has been committed, create
              // and replicate a final configuration containing only the new membership.
              final long index = log.appendEntry(new ConfigurationEntry(context.currentTerm(), members));

              // Replicate the final configuration to a majority of the cluster.
              writeMajority(index, new Handler<Boolean>() {
                @Override
                public void handle(Boolean succeeded) {
                  if (succeeded) {
                    // Once the new configuration has been replicated, apply
                    // the configuration to the current state and update the
                    // last applied index.
                    Leader.this.members = members;
                    Leader.this.remoteMembers = new HashSet<>(members);
                    Leader.this.remoteMembers.remove(context.address());

                    // Iterate through replicas and remove any replicas that
                    // were removed from the cluster.
                    Iterator<Replica> iterator = replicas.iterator();
                    while (iterator.hasNext()) {
                      Replica replica = iterator.next();
                      if (!remoteMembers.contains(replica.address)) {
                        replica.shutdown();
                        iterator.remove();
                        replicaMap.remove(replica.address);
                      }
                    }

                    // Release the configuration lock.
                    configLock.release();
                  }
                  else {
                    vertx.setTimer(1000, new Handler<Long>() {
                      @Override
                      public void handle(Long timerID) {
                        updateClusterConfig(members);
                      }
                    });
                  }
                }
              });
            }
            else {
              vertx.setTimer(1000, new Handler<Long>() {
                @Override
                public void handle(Long timerID) {
                  updateClusterConfig(members);
                }
              });
            }
          }
        });
      }
    });
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.currentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
    request.reply(context.currentTerm());
  }

  @Override
  public void sync(final SyncRequest request) {
    // If a newer term was provided by the request then sync as normal
    // and then step down as leader.
    if (request.term() > context.currentTerm()) {
      boolean result = doSync(request);

      // Once the new entries have been synchronized, step down.
      context.currentLeader(request.leader());
      context.currentTerm(request.term());
      context.transition(StateType.FOLLOWER);

      // Finally, respond to the sync request.
      request.reply(context.currentTerm(), result);
    }
    // Otherwise, we must have received some sync request from a node
    // that *thinks* it's the leader, but boy does it have another thing coming!
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void poll(final PollRequest request) {
    boolean result = doPoll(request);
    request.reply(context.currentTerm(), result);
  }

  @Override
  public void submit(final SubmitRequest request) {
    // If this is a read command then we need to contact a majority of the cluster
    // to ensure that the information is not stale. Once we've determined that
    // this node is the most up-to-date, we can simply apply the command to the
    // state machine and return the result without replicating the log.
    Command info = stateMachine.getCommand(request.command());
    if (info != null && info.type().equals(Command.Type.READ)) {

      // Users have the option of whether to require read majorities. If read
      // majorities are disabled then it is simply assumed that there are no
      // log conflicts since the last ping. This is safe in most cases since
      // log conflicts are unlikely in most cases. Nevertheless, read majorities
      // are required by default.
      if (context.requireReadMajority()) {
        readMajority(new Handler<Boolean>() {
          @Override
          public void handle(Boolean succeeded) {
            if (succeeded) {
              try {
                request.reply(stateMachine.applyCommand(request.command(), request.args()));
              }
              catch (Exception e) {
                request.error(e.getMessage());
              }
            }
            else {
              request.error("Failed to acquire majority replication.");
            }
          }
        });
      }
      // If read majorities are disabled then simply apply the command to the
      // state machine and return the result.
      else {
        try {
          request.reply(stateMachine.applyCommand(request.command(), request.args()));
        }
        catch (Exception e) {
          request.error(e.getMessage());
        }
      }
    }
    // Otherwise, for write commands or for commands for which a type was not
    // explicitly provided the entry must be replicated on a
    // majority of the cluster prior to responding to the request.
    else {
      // Append a new command entry to the log.
      final long index = log.appendEntry(new CommandEntry(context.currentTerm(), request.command(), request.args()));

      // Required write majorities are optional. Users may allow the log
      // to simply be replicated after the command is applied to the state
      // machine and the response is sent.
      if (context.requireWriteMajority()) {
        writeMajority(index, new Handler<Boolean>() {
          @Override
          public void handle(Boolean succeeded) {
            if (succeeded) {
              try {
                Object output = stateMachine.applyCommand(request.command(), request.args());
                request.reply(output);
              }
              catch (Exception e) {
                request.error(e.getMessage());
              }
            }
            else {
              request.error("Failed to acquire write majority.");
            }
          }
        });
      }
      // If write majorities are disabled then simply apply the command
      // to the state machine and return the result.
      else {
        try {
          Object output = stateMachine.applyCommand(request.command(), request.args());
          context.lastApplied(index);
          request.reply(output);
        }
        catch (Exception e) {
          request.error(e.getMessage());
        }
      }
    }
  }

  /**
   * Replicates the index to a majority of the cluster.
   */
  private void writeMajority(final long index, final Handler<Boolean> doneHandler) {
    final Majority majority = new Majority(remoteMembers).countSelf();
    majorities.add(majority);
    majority.start(new Handler<String>() {
      @Override
      public void handle(final String address) {
        if (replicaMap.containsKey(address)) {
          replicaMap.get(address).sync(index, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                majority.fail(address);
              }
              else {
                majority.succeed(address);
              }
            }
          });
        }
      }
    }, new Handler<Boolean>() {
      @Override
      public void handle(Boolean succeeded) {
        majorities.remove(majority);
        doneHandler.handle(succeeded);
      }
    });
  }

  /**
   * Pings a majority of the cluster.
   */
  private void readMajority(final Handler<Boolean> doneHandler) {
    final Majority majority = new Majority(remoteMembers).countSelf();
    majorities.add(majority);
    majority.start(new Handler<String>() {
      @Override
      public void handle(final String address) {
        if (replicaMap.containsKey(address)) {
          replicaMap.get(address).ping(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                majority.fail(address);
              }
              else {
                majority.succeed(address);
              }
            }
          });
        }
      }
    }, new Handler<Boolean>() {
      @Override
      public void handle(Boolean succeeded) {
        majorities.remove(majority);
        doneHandler.handle(succeeded);
      }
    });
  }

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    // Cancel the ping timer.
    if (pingTimer > 0) {
      vertx.cancelTimer(pingTimer);
      pingTimer = 0;
    }

    // Cancel any periodic retry timers.
    Iterator<Long> iterator = periodicTimers.iterator();
    while (iterator.hasNext()) {
      vertx.cancelTimer(iterator.next());
      iterator.remove();
    }

    // Cancel all majority input attempts.
    for (Majority majority : majorities) {
      majority.cancel();
    }

    // Shut down all replicas.
    for (Replica replica : replicas) {
      replica.shutdown();
    }

    // Stop observing the cluster configuration.
    config.deleteObserver(this);
    doneHandler.handle((Void) null);
  }

  /**
   * Determines which message have been committed.
   */
  private void checkCommits() {
    if (!replicas.isEmpty()) {
      // Sort the list of replicas, order by the last index that was replicated
      // to the replcica. This will allow us to determine the median index
      // for all known replicated entries across all cluster members.
      Collections.sort(replicas, new Comparator<Replica>() {
        @Override
        public int compare(Replica o1, Replica o2) {
          return Long.compare(o1.matchIndex, o2.matchIndex);
        }
      });
  
      // Set the current commit index as the median replicated index.
      // Since replicas is a list with zero based indexes, use the
      // floor(replicas size / 2) to get the middle most index. This
      // will ensure we get the replica which contains the log index
      // for the highest entry that has been replicated to a majority
      // of the cluster.
      context.commitIndex(replicas.get((int) Math.floor(replicas.size() / 2)).matchIndex);
    }
  }

  /**
   * A replica reference.
   */
  private class Replica {
    private final String address;
    private long nextIndex;
    private long matchIndex;
    private long lastPingTime;
    private boolean running;
    private boolean shutdown;
    private final Map<Long, Future<Void>> futures = new HashMap<>();

    private Replica(String address) {
      this.address = address;
      this.matchIndex = 0;
      this.nextIndex = log.lastIndex();
    }

    /**
     * Updates the replica, either synchronizing
     */
    private void update() {
      if (!shutdown) {
        if (nextIndex <= log.lastIndex()) {
          sync();
        }
        else {
          ping();
        }
      }
    }

    /**
     * Synchronizes the replica to a specific index.
     */
    private void sync(final long index, final Handler<AsyncResult<Void>> doneHandler) {
      futures.put(index, new DefaultFutureResult<Void>().setHandler(doneHandler));
      sync();
    }

    /**
     * Synchronizes the replica.
     */
    private void sync() {
      if (!running && !shutdown) {
        running = true;
        doSync();
      }
    }

    private void doSync() {
      if (shutdown) {
        running = false;
        return;
      }

      final long lastIndex = log.lastIndex();
      if (nextIndex <= lastIndex || matchIndex < context.commitIndex()) {
        if (nextIndex-1 > 0) {
          final long prevLogIndex = nextIndex - 1;
          Entry entry = log.getEntry(prevLogIndex);
          if (entry == null) {
            nextIndex--;
            doSync();
          }
          else {
            doSync(prevLogIndex, entry.term(), lastIndex);
          }
        }
        else {
          doSync(0, 0, lastIndex);
        }
      }
      else {
        running = false;
      }
    }

    private void doSync(final long prevLogIndex, final long prevLogTerm, final long lastIndex) {
      if (shutdown) {
        running = false;
        return;
      }

      // If there are entries to be synced then load the entries.
      if (prevLogIndex+1 <= lastIndex) {
        List<Entry> entries = log.getEntries(prevLogIndex+1, (prevLogIndex+1) + BATCH_SIZE > lastIndex ? lastIndex : (prevLogIndex+1) + BATCH_SIZE);
        doSync(prevLogIndex, prevLogTerm, entries, context.commitIndex());
      }
      // Otherwise, sync the commit index only.
      else {
        doSync(prevLogIndex, prevLogTerm, new ArrayList<Entry>(), context.commitIndex());
      }
    }

    private void doSync(final long prevLogIndex, final long prevLogTerm, final List<Entry> entries, final long commitIndex) {
      if (shutdown) {
        running = false;
        return;
      }

      if (logger.isDebugEnabled()) {
        if (!entries.isEmpty()) {
          if (entries.size() > 1) {
            logger.debug(String.format("%s replicating entries %d-%d to %s", context.address(), prevLogIndex+1, prevLogIndex+entries.size(), address));
          }
          else {
            logger.debug(String.format("%s replicating entry %d to %s", context.address(), prevLogIndex+1, address));
          }
        }
        else {
          logger.debug(String.format("%s committing entry %d to %s", context.address(), commitIndex, address));
        }
      }

      stateClient.sync(address, new SyncRequest(context.currentTerm(), context.address(), prevLogIndex, prevLogTerm, entries, commitIndex),
          context.heartbeatInterval() / 2, new Handler<AsyncResult<SyncResponse>>() {
        @Override
        public void handle(AsyncResult<SyncResponse> result) {
          if (result.succeeded()) {
            if (result.result().success()) {
              if (logger.isDebugEnabled()) {
                if (!entries.isEmpty()) {
                  if (entries.size() > 1) {
                    logger.debug(String.format("%s successfully replicated entries %d-%d to %s", context.address(), prevLogIndex+1, prevLogIndex+entries.size(), address));
                  }
                  else {
                    logger.debug(String.format("%s successfully replicated entry %d to %s", context.address(), prevLogIndex+1, address));
                  }
                }
                else {
                  logger.debug(String.format("%s successfully committed entry %d to %s", context.address(), commitIndex, address));
                }
              }

              // Update the next index to send and the last index known to be replicated.
              nextIndex = Math.max(nextIndex + 1, prevLogIndex + entries.size() + 1);
              matchIndex = Math.max(matchIndex, prevLogIndex + entries.size());

              // Trigger any futures related to the replicated entries.
              for (long i = prevLogIndex+1; i < (prevLogIndex+1) + entries.size(); i++) {
                if (futures.containsKey(i)) {
                  futures.remove(i).setResult((Void) null);
                }
              }

              // Update the current commit index and continue the synchronization.
              checkCommits();
              doSync();
            }
            else {
              if (logger.isDebugEnabled()) {
                if (!entries.isEmpty()) {
                  if (entries.size() > 1) {
                    logger.debug(String.format("%s failed to replicate entries %d-%d to %s", context.address(), prevLogIndex+1, prevLogIndex+entries.size(), address));
                  }
                  else {
                    logger.debug(String.format("%s failed to replicate entry %d to %s", context.address(), prevLogIndex+1, address));
                  }
                }
                else {
                  logger.debug(String.format("%s failed to commit entry %d to %s", context.address(), commitIndex, address));
                }
              }

              // If replication failed then decrement the next index and attemt to
              // retry replication. If decrementing the next index would result in
              // a next index of 0 then something must have gone wrong. Revert to
              // a follower.
              if (nextIndex-1 == 0) {
                running = false;
                context.transition(StateType.FOLLOWER);
              }
              else {
                // If we were attempting to replicate log entries and not just
                // sending a commit index or if we didn't have any log entries
                // to replicate then decrement the next index. The node we were
                // attempting to sync is not up to date.
                if (!entries.isEmpty() || prevLogIndex == commitIndex) {
                  nextIndex--;
                }
                checkCommits();
                doSync();
              }
            }
          }
          else {
            running = false;
            if (futures.containsKey(nextIndex)) {
              futures.remove(nextIndex).setFailure(result.cause());
            }
          }
        }
      });
    }

    /**
     * Pings the replica.
     */
    private void ping() {
      ping(null);
    }

    /**
     * Pings the replica, calling a handler once complete.
     */
    private void ping(final Handler<AsyncResult<Void>> doneHandler) {
      if (!shutdown) {
        final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
        final long startTime = System.currentTimeMillis();
        stateClient.ping(address, new PingRequest(context.currentTerm(), context.address()),
            context.useAdaptiveTimeouts() ? (lastPingTime > 0 ? (long) (lastPingTime * context.adaptiveTimeoutThreshold()) : context.heartbeatInterval() / 2) : context.heartbeatInterval() / 2,
                new Handler<AsyncResult<PingResponse>>() {
          @Override
          public void handle(AsyncResult<PingResponse> result) {
            if (result.succeeded()) {
              lastPingTime = System.currentTimeMillis() - startTime;
              if (result.result().term() > context.currentTerm()) {
                context.transition(StateType.FOLLOWER);
              }
              else {
                future.setResult((Void) null);
              }
            }
            else {
              future.setFailure(result.cause());
            }
          }
        });
      }
    }

    /**
     * Shuts down the replica.
     */
    private void shutdown() {
      shutdown = true;
    }
  }

}
