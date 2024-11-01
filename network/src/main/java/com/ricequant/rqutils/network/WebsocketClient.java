package com.ricequant.rqutils.network;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liche
 */
public class WebsocketClient {

  private final Vertx vertx;

  private final Int2ObjectMap<ServiceProcessor> processors = new Int2ObjectOpenHashMap<>();

  private final AtomicBoolean started = new AtomicBoolean(false);

  private long logicalSessionID = 0;

  private final String host;

  private final int port;

  private final String uri;

  private WebSocket socket;

  public WebsocketClient(Vertx vertx, String host, int port, String uri) {
    this.vertx = vertx;
    this.host = host;
    this.port = port;
    this.uri = uri;
  }

  public void attachService(ServiceProcessor processor) {
    int id = processor.id();
    synchronized (processors) {
      processors.put(id, processor);
    }
  }

  public void detachService(ServiceProcessor processor) {
    int id = processor.id();
    synchronized (processors) {
      processors.remove(id);
    }
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      this.vertx.createWebSocketClient().connect(this.port, this.host, this.uri).onSuccess(socket -> {
        this.socket = socket;
        if (logicalSessionID != 0) {
          Buffer writeBuffer = Buffer.buffer();
          writeBuffer.appendShort((short) 0).appendShort(NetConstants.RESUME_LOGICAL_SESSION).appendInt(0)
                  .appendLong(logicalSessionID);
          socket.writeBinaryMessage(writeBuffer);
        }

        socket.binaryMessageHandler(buf -> {

        });
      });
    }
  }

  public void stop() {
    if (started.compareAndSet(true, false)) {
      try {
        if (this.socket != null)
          this.socket.close();
      }
      catch (Exception e) {
        // ignore
      }
    }
  }
}
