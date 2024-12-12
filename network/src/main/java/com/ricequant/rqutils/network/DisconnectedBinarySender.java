package com.ricequant.rqutils.network;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

/**
 * @author liche
 */
public class DisconnectedBinarySender implements MixedServiceBinarySender {

  @Override
  public Future<Void> send(Buffer buffer) {
    return Future.failedFuture("Disconnected");
  }

  @Override
  public boolean connected() {
    return false;
  }
}
