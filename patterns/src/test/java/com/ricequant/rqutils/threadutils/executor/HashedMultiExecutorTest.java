package com.ricequant.rqutils.threadutils.executor;

import org.junit.jupiter.api.Disabled;

/**
 * @author chenfeng
 */
@Disabled
class HashedMultiExecutorTest {

  private HashedMultiExecutor exe;

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
    exe = new HashedMultiExecutor(3, 3);
  }

  @org.junit.jupiter.api.Test
  void queueTask() throws InterruptedException {
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
