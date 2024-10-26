package com.ricequant.rqutils.network;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liche
 */
public class WebsocketServer {

  private final Vertx vertx;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final Int2ObjectMap<ServiceProcessor> processors = new Int2ObjectOpenHashMap<>();

  private final AtomicBoolean started = new AtomicBoolean(false);

  private HttpServer server;

  private final Random random = new Random();

  private final Cache<Long, Long> disconnectedLogicalSessionID;

  public WebsocketServer(Vertx vertx) {
    this.vertx = vertx;
    disconnectedLogicalSessionID = Caffeine.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS).build();
  }

  public void start(int port) {
    if (started.compareAndSet(false, true)) {
      Future<HttpServer> serverFuture = vertx.createHttpServer().webSocketHandler(this::onNewSession).listen(port);
      CountDownLatch latch = new CountDownLatch(1);
      serverFuture.onSuccess(ret -> {
        this.server = ret;
        latch.countDown();
      }).onFailure(ex -> {
        ex.printStackTrace();
        System.exit(-1);
      });

      try {
        latch.await();
      }
      catch (InterruptedException e) {
        // ignore
      }
    }
  }

  public void stop() {
    if (started.compareAndSet(true, false)) {
      if (this.server != null) {
        CountDownLatch latch = new CountDownLatch(1);
        this.server.close().onComplete(v -> {
          if (v.failed())
            logger.error("Close server failed.", v.cause());
          synchronized (processors) {
            processors.clear();
          }
          latch.countDown();
        });

        try {
          latch.await();
        }
        catch (InterruptedException e) {
          // ignore
        }
      }
    }
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

  private void onNewSession(ServerWebSocket socket) {
    long[] logicalSessionID = new long[]{random.nextInt() ^ (System.currentTimeMillis() << 32)};

    synchronized (processors) {
      for (ServiceProcessor processor : processors.values()) {
        processor.onConnected(logicalSessionID[0], new MixedServiceWebsocketSender(processor.id(), socket));
      }
    }

    boolean[] logicalLoginSuccessful = new boolean[]{false};
    socket.handler(buffer -> {
      short operation = buffer.getShort(2);
      if (operation == NetConstants.NEW_LOGICAL_SESSION) {
        Buffer writeBuffer = Buffer.buffer();
        writeBuffer.appendShort((short) 0).appendShort(NetConstants.NEW_LOGICAL_SESSION).appendInt(0)
                .appendLong(logicalSessionID[0]);
        socket.writeBinaryMessage(writeBuffer);
        return;
      }

      if (operation == NetConstants.RESUME_LOGICAL_SESSION) {
        long proposedLogicalSessionID = buffer.getLong(8);
        if (disconnectedLogicalSessionID.getIfPresent(proposedLogicalSessionID) == null) {
          server.close();
        }
        else {
          logicalLoginSuccessful[0] = true;
          logicalSessionID[0] = proposedLogicalSessionID;
          Buffer writeBuffer = Buffer.buffer();
          writeBuffer.appendShort((short) 0).appendShort(NetConstants.RESUME_LOGICAL_SESSION).appendInt(0)
                  .appendLong(logicalSessionID[0]);
          socket.writeBinaryMessage(writeBuffer);
          synchronized (processors) {
            for (ServiceProcessor processor : processors.values()) {
              processor.onConnected(logicalSessionID[0], new MixedServiceWebsocketSender(processor.id(), socket));
            }
          }
        }
        return;
      }

      short serviceID = buffer.getShort(0);
      ServiceProcessor processor;
      synchronized (processors) {
        processor = processors.get(serviceID);
      }
      if (processor != null) {
        ByteBuffer toDecode = buffer.getByteBuf().nioBuffer().position(16);
        try {
          processor.process(logicalSessionID[0], toDecode);
        }
        catch (Throwable t) {
          logger.error("Message processor error", t);
        }
      }
      else {
        Buffer writeBuffer = Buffer.buffer();
        writeBuffer.appendShort(serviceID).appendShort(NetConstants.SERVICE_NOT_FOUND).appendInt(0)
                .appendLong(logicalSessionID[0]);
        socket.writeBinaryMessage(writeBuffer);
        logicalLoginSuccessful[0] = false;
        socket.close();
      }
    });

    socket.closeHandler(v -> {
      if (logicalLoginSuccessful[0]) {
        disconnectedLogicalSessionID.put(logicalSessionID[0], logicalSessionID[0]);
        synchronized (processors) {
          for (ServiceProcessor processor : processors.values()) {
            processor.onDisconnected(logicalSessionID[0]);
          }
        }
      }
    });
  }
}
