package com.ricequant.rqutils.threadutils.executor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kangol
 */
public class StoppableSingleTask {

  private final Thread thread;

  private final AtomicBoolean running = new AtomicBoolean(true);

  private static final AtomicInteger threadCounter = new AtomicInteger(0);

  private StoppableSingleTask(Runnable runnable, String name) {
    if (name == null)
      name = "StoppableSingleTask-" + threadCounter.incrementAndGet();
    thread = new Thread(() -> {
      while (running.get()) {
        try {
          runnable.run();
        }
        catch (Throwable e) {
          e.printStackTrace();
          break;
        }
      }
    }, name);

    thread.start();
  }

  public static StoppableSingleTask run(Runnable runnable) {
    return new StoppableSingleTask(runnable, null);
  }

  public static StoppableSingleTask run(Runnable runnable, String name) {
    return new StoppableSingleTask(runnable, name);
  }

  public void stop() {
    running.set(false);
    thread.interrupt();
  }
}
