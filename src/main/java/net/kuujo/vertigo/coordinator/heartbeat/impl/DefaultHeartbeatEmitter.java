/*
 * Copyright 2013 the original author or authors.
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
package net.kuujo.vertigo.coordinator.heartbeat.impl;

import net.kuujo.vertigo.context.InstanceContext;
import net.kuujo.vertigo.coordinator.heartbeat.HeartbeatEmitter;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * Default heartbeat emitter implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultHeartbeatEmitter implements HeartbeatEmitter {
  private String address;
  private Vertx vertx;
  private final Logger log;
  private EventBus eventBus;
  private long interval = 1000;
  private long timerID;

  public DefaultHeartbeatEmitter(Vertx vertx, InstanceContext<?> context) {
    this.vertx = vertx;
    this.log = LoggerFactory.getLogger(HeartbeatEmitter.class.getName() + "-" + context);
    this.eventBus = vertx.eventBus();
  }

  public DefaultHeartbeatEmitter(String address, Vertx vertx, InstanceContext<?> context) {
    this.address = address;
    this.vertx = vertx;
    this.log = LoggerFactory.getLogger(HeartbeatEmitter.class.getName() + "-" + context);
    this.eventBus = vertx.eventBus();
  }

  @Override
  public HeartbeatEmitter setAddress(String address) {
    this.address = address;
    return this;
  }

  @Override
  public String getAddress() {
    return address;
  }

  @Override
  public HeartbeatEmitter setInterval(long interval) {
    this.interval = interval;
    return this;
  }

  @Override
  public long getInterval() {
    return interval;
  }

  @Override
  public void start() {
    if (log.isInfoEnabled()) {
      log.info(String.format("Starting heartbeats to %s", address));
    }

    timerID = vertx.setPeriodic(interval, new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        eventBus.send(address, true);
        if (log.isDebugEnabled()) {
          log.debug(String.format("Sent heartbeat message to %s", address));
        }
      }
    });

    if (log.isDebugEnabled()) {
      log.debug(String.format("Set periodic heartbeat timer %d", timerID));
    }
  }

  @Override
  public void stop() {
    if (log.isInfoEnabled()) {
      log.info(String.format("Stopping heartbeats to %s", address));
    }
    if (timerID != 0) {
      if (log.isDebugEnabled()) {
        log.debug(String.format("Cancelling periodic heartbeat timer %d", timerID));
      }
      vertx.cancelTimer(timerID);
    }
  }

}
