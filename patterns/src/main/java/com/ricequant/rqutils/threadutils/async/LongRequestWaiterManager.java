package com.ricequant.rqutils.threadutils.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * @author kangol
 */
public class LongRequestWaiterManager<KeyType, ResultType> {

  private final Map<KeyType, List<BiConsumer<ResultType, Throwable>>> resultWaiters = new ConcurrentHashMap<>();

  private final Map<KeyType, ResultType> results = new ConcurrentHashMap<>();

  public void onComplete(KeyType key, ResultType result) {
    synchronized (results) {
      results.put(key, result);
      synchronized (resultWaiters) {
        List<BiConsumer<ResultType, Throwable>> waiters = resultWaiters.get(key);
        if (waiters != null) {
          for (var w : waiters) {
            w.accept(result, null);
          }
          waiters.clear();
        }
      }
    }
  }

  public int await(KeyType key, BiConsumer<ResultType, Throwable> listener) {
    ResultType result = results.get(key);
    int numWaiters = 0;
    if (result == null) {
      synchronized (results) {
        result = results.get(key);
        if (result == null) {
          List<BiConsumer<ResultType, Throwable>> waiters = resultWaiters.get(key);
          if (waiters == null) {
            synchronized (resultWaiters) {
              waiters = resultWaiters.computeIfAbsent(key, k -> new ArrayList<>());
            }
          }
          numWaiters = waiters.size();
          waiters.add(listener);
        }
      }
    }

    if (result != null) {
      listener.accept(result, null);
    }
    return numWaiters;
  }

  public void onFail(KeyType key, Throwable error) {
    synchronized (resultWaiters) {
      List<BiConsumer<ResultType, Throwable>> waiters = resultWaiters.get(key);
      if (waiters != null) {
        for (var w : waiters) {
          w.accept(null, error);
        }
        waiters.clear();
      }
    }
  }
}
