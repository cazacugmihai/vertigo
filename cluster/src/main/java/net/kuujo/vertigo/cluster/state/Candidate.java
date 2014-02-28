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

import net.kuujo.vertigo.cluster.log.Entry;
import net.kuujo.vertigo.cluster.protocol.PingRequest;
import net.kuujo.vertigo.cluster.protocol.PollRequest;
import net.kuujo.vertigo.cluster.protocol.PollResponse;
import net.kuujo.vertigo.cluster.protocol.SubmitRequest;
import net.kuujo.vertigo.cluster.protocol.SyncRequest;
import net.kuujo.vertigo.cluster.state.impl.Majority;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A candidate state.
 * 
 * @author Jordan Halterman
 */
class Candidate extends State {
  private static final Logger logger = LoggerFactory.getLogger(Candidate.class);
  private Majority majority;
  private long electionTimer;

  @Override
  public void startUp(Handler<Void> doneHandler) {
    // When the candidate is created, increment the current term.
    context.currentTerm(context.currentTerm() + 1);
    doneHandler.handle((Void) null);
    vertx.runOnContext(new Handler<Void>() {
      @Override
      public void handle(Void _) {
        logger.info("Starting election.");
        resetTimer();
        pollMembers();
      }
    });
  }

  /**
   * Resets the election timer.
   */
  private void resetTimer() {
    electionTimer = vertx.setTimer(
        context.electionTimeout() - (context.electionTimeout() / 4) + (Math.round(Math.random() * (context.electionTimeout() / 2))),
        new Handler<Long>() {
          @Override
          public void handle(Long timerID) {
            // When the election times out, clear the previous majority vote
            // check and restart the election.
            logger.info("Election timed out.");
            if (majority != null) {
              majority.cancel();
              majority = null;
            }
            resetTimer();
            pollMembers();
            logger.info("Restarted election.");
          }
        });
  }

  private void pollMembers() {
    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    if (majority == null) {
      majority = new Majority(context.members());
      majority.start(new Handler<String>() {
        @Override
        public void handle(final String address) {
          // Load the last log entry to get its term. We do this rather
          // than
          // calling lastTerm() because the last entry could have changed
          // already.
          final long lastIndex = log.lastIndex();
          final Entry entry = log.getEntry(lastIndex);
          long lastTerm = 0;
          if (entry != null) {
            lastTerm = entry.term();
          }

          stateClient.poll(address, new PollRequest(context.currentTerm(), context.address(), lastIndex, lastTerm),
              new Handler<AsyncResult<PollResponse>>() {
                @Override
                public void handle(AsyncResult<PollResponse> result) {
                  // If the election is null then that means it was
                  // already finished,
                  // e.g. a majority of nodes responded.
                  if (majority != null) {
                    if (result.failed() || !result.result().voteGranted()) {
                      majority.fail(address);
                    }
                    else {
                      majority.succeed(address);
                    }
                  }
                }
              });
        }
      }, new Handler<Boolean>() {
        @Override
        public void handle(Boolean elected) {
          majority = null;
          if (elected) {
            context.transition(StateType.LEADER);
          }
          else {
            context.transition(StateType.FOLLOWER);
          }
        }
      });
    }
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.currentLeader(request.leader());
      context.currentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
    request.reply(context.currentTerm());
  }

  @Override
  public void sync(final SyncRequest request) {
    if (doSync(request)) {
      if (request.term() > context.currentTerm()) {
        context.currentLeader(request.leader());
        context.currentTerm(request.term());
        context.transition(StateType.FOLLOWER);
      }

      // Reply to the request.
      request.reply(context.currentTerm(), true);
    }
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void poll(PollRequest request) {
    if (request.candidate().equals(context.address())) {
      request.reply(context.currentTerm(), true);
      context.votedFor(context.address());
    }
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void submit(SubmitRequest request) {
    request.error("Not a leader.");
  }

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    if (electionTimer > 0) {
      vertx.cancelTimer(electionTimer);
      electionTimer = 0;
    }
    if (majority != null) {
      majority.cancel();
      majority = null;
    }
    doneHandler.handle((Void) null);
  }

}
