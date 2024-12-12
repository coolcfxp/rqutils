package com.ricequant.rqutils.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;

/**
 * @author liche
 */
public class MixedServiceWebsocketSender implements MixedServiceBinarySender {

  private final int serviceID;

  private final ServerWebSocket socket;

  public MixedServiceWebsocketSender(int serviceID, ServerWebSocket socket) {
    this.serviceID = serviceID;
    this.socket = socket;
  }

  private final ThreadLocal<ByteBuf> nettyBuffer =
          ThreadLocal.withInitial(() -> PooledByteBufAllocator.DEFAULT.ioBuffer(512, 1024 * 64));

  private final ThreadLocal<Buffer> vertxBuffer = ThreadLocal.withInitial(() -> Buffer.buffer(nettyBuffer.get()));

  @Override
  public Future<Void> send(Buffer buffer) {
    nettyBuffer.get().clear();
    vertxBuffer.get().appendInt(serviceID).setBuffer(16, buffer);
    return socket.writeBinaryMessage(vertxBuffer.get());
  }

  @Override
  public boolean connected() {
    return !this.socket.isClosed();
  }
}
