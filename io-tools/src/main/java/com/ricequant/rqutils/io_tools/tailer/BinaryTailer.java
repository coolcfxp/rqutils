package com.ricequant.rqutils.io_tools.tailer;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kangol
 */
public class BinaryTailer {

  public static class Builder {

    private String file;

    private int bufferSize = 4096;

    private BinaryTailerListener listener;

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

    public BinaryTailer build() {
      return new BinaryTailer(file, bufferSize, listener, 500);
    }
  }

  private final File file;

  private final int bufferSize;

  private final BinaryTailerListener listener;

  private final int interval;

  private long lastPos;

  private final RandomAccessFile raFile;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private BinaryTailer(String file, int bufferSize, BinaryTailerListener listener, int interval) {
    if (file == null)
      throw new IllegalArgumentException("file is null");
    if (bufferSize <= 0)
      throw new IllegalArgumentException("bufferSize is not positive");
    if (listener == null)
      throw new IllegalArgumentException("listener is null");

    this.file = new File(file);
    if (!this.file.exists())
      throw new IllegalArgumentException("file does not exist");
    if (!this.file.isFile())
      throw new IllegalArgumentException("file is not a file");
    if (!this.file.canRead())
      throw new IllegalArgumentException("file is not readable");

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
  }

  public void start() {
    running.set(true);
    new Thread(() -> {
      long lastModified = 0;
      byte[] readBuffer = new byte[bufferSize];
      while (running.get()) {
        try {
          Thread.sleep(interval);
          if (file.lastModified() == lastModified || lastPos == file.length())
            continue;

          lastModified = file.lastModified();

          try {
            this.raFile.seek(lastPos);
            int maxReadLength = (int) (file.length() - lastPos);
            if (maxReadLength > bufferSize)
              maxReadLength = bufferSize;
            if (maxReadLength <= 0)
              continue;

            int readLength = this.raFile.read(readBuffer, 0, maxReadLength);
            if (readLength > 0) {
              listener.onNewData(readBuffer, 0, readLength);
              this.lastPos += readLength;
            }
          }
          catch (EOFException e) {
            this.lastPos = file.length();
          }
          catch (IOException e) {
            this.listener.onFileError(e);
          }
        }
        catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
      }
      try {
        this.raFile.close();
      }
      catch (IOException e) {
        // ignore
      }
    }).start();
  }

  public void stop() {
    running.set(false);
  }

}
