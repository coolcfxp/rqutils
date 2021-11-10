package com.ricequant.rqutils.threadutils.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chenfeng
 */
public class HashedMultiExecutor {

  private final List<ArrayBlockingQueue<Runnable>> taskQueues;

  private final Thread[] threads;

  private final AtomicBoolean[] startedIndicators;

  public HashedMultiExecutor(int numExecutors, int queueSize) {
    numExecutors = numExecutors > 0 ? numExecutors : 1;
    startedIndicators = new AtomicBoolean[numExecutors];
    for (int i = 0; i < startedIndicators.length; i++) {
      startedIndicators[i] = new AtomicBoolean(false);
    }

    taskQueues = new ArrayList<>(numExecutors);
    threads = new Thread[numExecutors];

    for (int i = 0; i < threads.length; i++) {
      ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueSize);
      taskQueues.add(queue);
      AtomicBoolean started = startedIndicators[i];
      threads[i] = new Thread(() -> {
        while (started.get()) {
          try {
            queue.take().run();
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  public HashedMultiExecutor() {
    this(Runtime.getRuntime().availableProcessors() - 1, 1024);
  }

  public int numExecutors() {
    return threads.length;
  }

  public void startAll() {
    for (int i = 0; i < threads.length; i++)
      start(i);
  }

  public void start(int num) {
    startedIndicators[num].set(true);
    threads[num].start();
  }

  public void queueTask(Runnable task, int hash) throws InterruptedException {
    int num = hash % taskQueues.size();
    taskQueues.get(num).put(task);
  }

  public CompletableFuture<String> stopAll() {
    CompletableFuture<String> future = new CompletableFuture<>();
    Thread stoppingThread = new Thread(() -> {
      for (int i = 0; i < threads.length; i++) {
        try {
          stop(i).join();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
      future.complete(null);
    });
    stoppingThread.start();
    return future;
  }

  public CompletableFuture<String> stop(int num) {
    CompletableFuture<String> future = new CompletableFuture<>();
    try {
      taskQueues.get(num).put(() -> {
        startedIndicators[num].set(false);
        future.complete(null);
      });
    }
    catch (InterruptedException e) {
      e.printStackTrace();
      future.completeExceptionally(e);
    }

    return future;
  }
}
