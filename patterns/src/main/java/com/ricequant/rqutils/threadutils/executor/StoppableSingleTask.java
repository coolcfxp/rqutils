package com.ricequant.rqutils.threadutils.executor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kangol
 */
public class StoppableSingleTask {

  private final Thread thread;

  private final AtomicBoolean running = new AtomicBoolean(true);

  private StoppableSingleTask(Runnable runnable) {
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
    });

    thread.start();
  }

  public static StoppableSingleTask run(Runnable runnable) {
    return new StoppableSingleTask(runnable);
  }

  public void stop() {
    running.set(false);
    thread.interrupt();
  }
}
