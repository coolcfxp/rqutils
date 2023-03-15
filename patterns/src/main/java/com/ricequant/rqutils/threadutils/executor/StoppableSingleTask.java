package com.ricequant.rqutils.threadutils.executor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author kangol
 */
public class StoppableSingleTask {

  private final Thread thread;

  private final AtomicBoolean running = new AtomicBoolean(true);

  private static final AtomicInteger threadCounter = new AtomicInteger(0);

  private StoppableSingleTask(Supplier<Boolean> runnable, String name) {
    if (name == null)
      name = "StoppableSingleTask-" + threadCounter.incrementAndGet();
    thread = new Thread(() -> {
      while (running.get()) {
        try {
          if (runnable.get()) {
            break;
          }
        }
        catch (Throwable e) {
          e.printStackTrace();
          break;
        }
      }
    }, name);

    thread.start();
  }

  public static StoppableSingleTask run(Supplier<Boolean> runnable) {
    return new StoppableSingleTask(runnable, null);
  }

  public static StoppableSingleTask run(Supplier<Boolean> runnable, String name) {
    return new StoppableSingleTask(runnable, name);
  }

  public void stop() {
    running.set(false);
    thread.interrupt();
  }
}
