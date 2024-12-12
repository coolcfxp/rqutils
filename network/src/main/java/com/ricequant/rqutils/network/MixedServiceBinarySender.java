package com.ricequant.rqutils.network;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

/**
 * @author liche
 */
public interface MixedServiceBinarySender {

  Future<Void> send(Buffer buffer);

  boolean connected();
}
