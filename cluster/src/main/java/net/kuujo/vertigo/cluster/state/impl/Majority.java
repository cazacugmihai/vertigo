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
package net.kuujo.vertigo.cluster.state.impl;

import java.util.Set;

import org.vertx.java.core.Handler;

/**
 * A quorum helper.
 * 
 * @author Jordan Halterman
 */
public final class Majority {
  private final Set<String> members;
  private int total;
  private int succeeded;
  private int failed;
  private boolean complete;
  private Handler<Boolean> doneHandler;

  public Majority(Set<String> members) {
    this.members = members;
    total = members.size();
  }

  public Majority start(Handler<String> handler, Handler<Boolean> doneHandler) {
    if (members.size() > 0) {
      for (String address : members) {
        handler.handle(address);
      }
      this.doneHandler = doneHandler;
    }
    else {
      doneHandler.handle(true);
    }
    return this;
  }

  public Majority countSelf() {
    total++;
    succeeded++;
    return this;
  }

  private void checkComplete() {
    if (!complete && doneHandler != null) {
      if (succeeded > total / 2) {
        complete = true;
        doneHandler.handle(true);
      }
      else if (failed > total / 2) {
        complete = true;
        doneHandler.handle(false);
      }
    }
  }

  public Majority succeed(String address) {
    succeeded++;
    checkComplete();
    return this;
  }

  public Majority fail(String address) {
    failed++;
    checkComplete();
    return this;
  }

  public Majority cancel() {
    doneHandler = null;
    return this;
  }

}
