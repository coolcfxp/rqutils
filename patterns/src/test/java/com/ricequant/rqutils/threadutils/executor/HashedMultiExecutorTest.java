package com.ricequant.rqutils.threadutils.executor;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author chenfeng
 */
class HashedMultiExecutorTest {

  private HashedMultiExecutor exe;

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
    exe = new HashedMultiExecutor(3, 3);
  }

  @org.junit.jupiter.api.Test
  void queueTask() throws InterruptedException {
    exe.startAll();
    exe.queueTask(() -> {
      sleep(1);
    }, 1);

    exe.queueTask(() -> {
      sleep(2);
    }, 2);

    exe.queueTask(() -> {
      sleep(3);
    }, 3);

    System.out.println(exe.stopAll().join());
  }

  private void sleep(int s) {
    try {
      Thread.sleep(s * 1000L);
      System.out.println("wake: " + s);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
