package com.ricequant.rqutils.io_tools.tailer;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kangol
 */
public class BinaryTailer {

  private final static int DEFAULT_INTERVAL = 100;

  private ScheduledFuture<?> task;

  private long lastModified;

  private byte[] readBuffer;

  public static class Builder {

    private String file;

    private int bufferSize = 4096;

    private BinaryTailerListener listener;

    private ThreadFactory schedulerThreadFactory = Executors.defaultThreadFactory();

    public Builder file(String file) {
      this.file = file;
      return this;
    }

    public Builder bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder listener(BinaryTailerListener listener) {
      this.listener = listener;
      return this;
    }

    public Builder schedulerThreadFactory(ThreadFactory schedulerThreadFactory) {
      this.schedulerThreadFactory = schedulerThreadFactory;
      return this;
    }

    public BinaryTailer build() {
      return new BinaryTailer(file, bufferSize, listener, DEFAULT_INTERVAL, schedulerThreadFactory);
    }
  }

  private final File file;

  private final int bufferSize;

  private final BinaryTailerListener listener;

  private final int interval;

  private long lastPos;

  private final RandomAccessFile raFile;

  private final ScheduledExecutorService scheduler;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private BinaryTailer(String file, int bufferSize, BinaryTailerListener listener, int interval,
          ThreadFactory schedulerThreadFactory) {
    if (file == null)
      throw new IllegalArgumentException("file is null");
    if (bufferSize <= 0)
      throw new IllegalArgumentException("bufferSize is not positive");
    if (listener == null)
      throw new IllegalArgumentException("listener is null");

    this.file = new File(file);
    if (!this.file.exists())
      throw new IllegalArgumentException("file does not exist: " + file);
    if (!this.file.isFile())
      throw new IllegalArgumentException("file is not a file: " + file);
    if (!this.file.canRead())
      throw new IllegalArgumentException("file is not readable: " + file);

    this.bufferSize = bufferSize;
    this.listener = listener;
    this.interval = interval;
    this.lastPos = 0;

    try {
      this.raFile = new RandomAccessFile(file, "r");
    }
    catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

    this.readBuffer = new byte[bufferSize];

    if (schedulerThreadFactory != null)
      scheduler = Executors.newSingleThreadScheduledExecutor(schedulerThreadFactory);
    else
      scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  public void startPeriodicalScan() {
    if (!running.compareAndSet(false, true))
      return;
    this.lastModified = 0;
    this.task = scheduler.scheduleAtFixedRate(() -> {
      if (!running.get()) {
        this.task.cancel(true);
        close();
        return;
      }

      scan();
    }, 0, interval, TimeUnit.MILLISECONDS);
  }

  public void scan() {
    if (file.lastModified() == lastModified || lastPos == file.length())
      return;

    lastModified = file.lastModified();
    try {
      this.raFile.seek(lastPos);
      int lengthLeft = (int) (file.length() - lastPos);
      if (lengthLeft <= 0)
        return;
      int totalRead = 0;
      while (totalRead < lengthLeft) {
        int maxReadLength = Math.min(lengthLeft - totalRead, bufferSize);
        try {
          listener.onBeforeRead(this.raFile);
        }
        catch (Throwable e) {
          e.printStackTrace();
        }
        this.raFile.seek(this.lastPos);
        int readLength = this.raFile.read(readBuffer, 0, maxReadLength);
        if (readLength > 0) {
          try {
            listener.onNewData(readBuffer, 0, readLength);
          }
          catch (Throwable e) {
            e.printStackTrace();
          }
          this.lastPos += readLength;
          totalRead += readLength;
        }
        else {
          break;
        }
      }
    }
    catch (EOFException e) {
      this.lastPos = file.length();
    }
    catch (IOException e) {
      this.listener.onFileError(e);
    }
  }

  public void close() {
    try {
      this.raFile.close();
    }
    catch (IOException e) {
      // ignore
    }
  }

  public void stopPeriodicalScan() {
    running.set(false);
  }

}
