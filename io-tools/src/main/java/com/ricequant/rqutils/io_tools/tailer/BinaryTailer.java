package com.ricequant.rqutils.io_tools.tailer;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kangol
 */
public class BinaryTailer {

  private final static int DEFAULT_INTERVAL = 100;

  private final MessageDigest md;

  private final File comparisonFailedFileContentBackup;

  private long lastOpen;

  private ScheduledFuture<?> task;

  private long lastModified;

  private byte[] readBuffer;

  private long reopenInterval = 0;

  private int writeCount;

  public static class Builder {

    private String file;

    private int bufferSize = 4096;

    private BinaryTailerListener listener;

    private ThreadFactory schedulerThreadFactory = Executors.defaultThreadFactory();

    private int reopenInterval = 0;

    private int rescanInterval = DEFAULT_INTERVAL;

    private boolean compareFileContentWhenReopen = false;

    private File comparisonFailedFileContentBackup = null;

    public Builder file(String file) {
      this.file = file;
      return this;
    }

    public Builder bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder reopenInterval(int interval) {
      this.reopenInterval = interval;
      return this;
    }

    public Builder rescanInterval(int interval) {
      this.rescanInterval = interval;
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

    public Builder compareFileContentWhenReopen(boolean compareFileContentWhenReopen) {
      this.compareFileContentWhenReopen = compareFileContentWhenReopen;
      return this;
    }

    public Builder comparisonFailedFileContentBackup(File comparisonFailedFileContentBackup) {
      this.comparisonFailedFileContentBackup = comparisonFailedFileContentBackup;
      return this;
    }

    public BinaryTailer build() {
      return new BinaryTailer(file, bufferSize, listener, rescanInterval, reopenInterval, compareFileContentWhenReopen,
              comparisonFailedFileContentBackup, schedulerThreadFactory);
    }
  }

  private final File file;

  private final int bufferSize;

  private final BinaryTailerListener listener;

  private final int interval;

  private long lastPos;

  private RandomAccessFile raFile;

  private final ScheduledExecutorService scheduler;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private BinaryTailer(String file, int bufferSize, BinaryTailerListener listener, int interval, int reopenInterval,
          boolean compareFileContentWhenReopen, File comparisonFailedFileContentBackup,
          ThreadFactory schedulerThreadFactory) {
    if (file == null)
      throw new IllegalArgumentException("file is null");
    if (bufferSize <= 0)
      throw new IllegalArgumentException("bufferSize is not positive");
    if (listener == null)
      throw new IllegalArgumentException("listener is null");

    this.file = new File(file);
    if (!this.file.exists())
      throw new IllegalArgumentException("file does not exist: " + this.file.getAbsolutePath());
    if (!this.file.isFile())
      throw new IllegalArgumentException("file is not a file: " + this.file.getAbsolutePath());
    if (!this.file.canRead())
      throw new IllegalArgumentException("file is not readable: " + this.file.getAbsolutePath());

    MessageDigest localMD = null;
    if (compareFileContentWhenReopen) {
      try {
        localMD = MessageDigest.getInstance("MD5");
      }
      catch (NoSuchAlgorithmException e) {
        System.err.println("Unable to find Hash algorithm MD5");
        e.printStackTrace();
      }
    }

    this.md = localMD;
    this.comparisonFailedFileContentBackup = comparisonFailedFileContentBackup;
    this.bufferSize = bufferSize;
    this.listener = listener;
    this.interval = interval;
    this.reopenInterval = reopenInterval;
    this.lastPos = 0;

    try {
      this.raFile = new RandomAccessFile(file, "r");
      this.lastOpen = System.currentTimeMillis();
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
    long now = System.currentTimeMillis();
    if (reopenInterval > 0 && now - this.lastOpen >= this.reopenInterval) {
      try {
        byte[] lastHash = null;
        byte[] buffer = null;
        if (this.md != null) {
          this.raFile.seek(0);
          buffer = new byte[(int) lastPos + 1];
          this.raFile.read(buffer, 0, (int) lastPos + 1);
          this.raFile.seek(lastPos);
          lastHash = this.md.digest(buffer);
        }

        this.raFile.close();
        Thread.sleep(1);
        this.raFile = new RandomAccessFile(file, "r");
        this.raFile.seek(0);

        if (this.md != null) {
          // reuse buffer to save space if it is not necessary to output file content
          byte[] buffer2 = this.comparisonFailedFileContentBackup == null ? buffer : new byte[(int) lastPos + 1];

          this.raFile.read(buffer2, 0, (int) Math.min(this.raFile.length(), lastPos + 1));
          byte[] currentHash = this.md.digest(buffer2);

          boolean isSameHash = true;
          for (int i = 0; i < lastHash.length; i++) {
            if (currentHash[i] != lastHash[i]) {
              isSameHash = false;
              break;
            }
          }

          if (!isSameHash) {
            System.err.println("File last open has different content in the first " + (lastPos + 1) + " bytes.");
            if (this.comparisonFailedFileContentBackup != null && this.writeCount < 3) {
              File originalFile = new File(this.comparisonFailedFileContentBackup, now + "_original");
              if (originalFile.createNewFile())
                FileUtils.writeByteArrayToFile(originalFile, buffer);
              File reopenedFile = new File(this.comparisonFailedFileContentBackup, now + "_reopened");
              if (originalFile.createNewFile())
                FileUtils.writeByteArrayToFile(reopenedFile, buffer2);
              this.writeCount++;
            }
          }
        }

        this.raFile.seek(lastPos);
        this.lastOpen = now;
      }
      catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    if (file.lastModified() == lastModified && lastPos == file.length())
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
