package com.ricequant.rqutils.threadutils.id;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kangol
 */
public enum LongIDGenerator {
  INSTANCE;

  private final AtomicLong id = new AtomicLong(System.currentTimeMillis());

  public long next() {
    return id.incrementAndGet();
  }
}
