package com.ricequant.rqutils.threadutils.executor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kangol
 */
public class StoppableSingleTask {

  private final Thread thread;

  private final AtomicBoolean started = new AtomicBoolean(true);

  private StoppableSingleTask(Runnable runnable) {
    thread = new Thread(() -> {
      while (started.get()) {
        try {
          runnable.run();
        }
        catch (Throwable e) {
          break;
        }
      }
    });

    thread.start();
  }

  public static StoppableSingleTask start(Runnable runnable) {
    return new StoppableSingleTask(runnable);
  }

  public void stop() {
    started.set(false);
    thread.interrupt();
  }
}
