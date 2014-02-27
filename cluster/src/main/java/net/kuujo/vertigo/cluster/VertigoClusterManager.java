package net.kuujo.vertigo.cluster;

import java.util.UUID;

import net.kuujo.vertigo.cluster.impl.DefaultClusterManagerService;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.platform.Verticle;

/**
 * Cluster manager verticle.
 *
 * @author Jordan Halterman
 */
public class VertigoClusterManager extends Verticle {
  private String address;
  private String cluster;
  private ClusterManagerService service;

  @Override
  public void start(final Future<Void> startResult) {
    address = container.config().getString("address", UUID.randomUUID().toString());
    cluster = container.config().getString("cluster", "vertigo");
    service = new DefaultClusterManagerService(address, cluster, vertx, container);
    service.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          startResult.setFailure(result.cause());
        }
        else {
          VertigoClusterManager.super.start(startResult);
        }
      }
    });
  }

}
