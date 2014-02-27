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

import net.kuujo.vertigo.cluster.protocol.PingRequest;
import net.kuujo.vertigo.cluster.protocol.PollRequest;
import net.kuujo.vertigo.cluster.protocol.SubmitRequest;
import net.kuujo.vertigo.cluster.protocol.SyncRequest;

import org.vertx.java.core.Handler;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A follower state.
 * 
 * @author Jordan Halterman
 */
class Follower extends State {
  private static final Logger logger = LoggerFactory.getLogger(Follower.class);
  private long timeoutTimer;

  /**
   * An election timeout handler.
   */
  private final Handler<Long> timeoutHandler = new Handler<Long>() {
    @Override
    public void handle(Long timerID) {
      // If the node has not yet voted for anyone then transition to
      // candidate and start a new election.
      if (context.votedFor() == null) {
        logger.info("Election timed out. Transitioning to candidate.");
        context.transition(StateType.CANDIDATE);
        timeoutTimer = 0;
      }
      // Otherwise, if the node voted for a candidate then reset the election
      // timer.
      else {
        resetTimer();
      }
    }
  };

  @Override
  public void startUp(Handler<Void> doneHandler) {
    resetTimer();
    doneHandler.handle((Void) null);
  }

  /**
   * Resets the internal election timer.
   */
  private void resetTimer() {
    if (timeoutTimer > 0) {
      vertx.cancelTimer(timeoutTimer);
    }
    // Set the election timeout in a semi-random fashion with the random range
    // being somewhere between .75 * election timeout and 1.25 * election
    // timeout.
    timeoutTimer = vertx.setTimer(context.electionTimeout() - (context.electionTimeout() / 4)
        + (Math.round(Math.random() * (context.electionTimeout() / 2))), timeoutHandler);
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.currentLeader(request.leader());
      context.currentTerm(request.term());
    }
    request.reply(context.currentTerm());
    resetTimer();
  }

  @Override
  public void sync(final SyncRequest request) {
    if (doSync(request)) {
      // If the request term is greater than the current term then update
      // the current leader and term.
      if (request.term() > context.currentTerm()) {
        context.currentLeader(request.leader());
        context.currentTerm(request.term());
      }

      // Reply to the request.
      request.reply(context.currentTerm(), true);

      // Reset the election timer.
      resetTimer();
    }
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void poll(final PollRequest request) {
    boolean result = doPoll(request);
    request.reply(context.currentTerm(), result);
    resetTimer();
  }

  @Override
  public void submit(SubmitRequest request) {
    // This node should never receive a submit request. All submits should
    // be automatically forwarded to the leader.
    request.error("Not a leader.");
  }

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    if (timeoutTimer > 0) {
      vertx.cancelTimer(timeoutTimer);
      timeoutTimer = 0;
    }
    doneHandler.handle((Void) null);
  }

}
