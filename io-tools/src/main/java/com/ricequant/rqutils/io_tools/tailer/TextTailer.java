package com.ricequant.rqutils.io_tools.tailer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kangol
 */
public class TextTailer {

  private final static int DEFAULT_INTERVAL = 500;

  private ScheduledFuture<?> task;

  private long lastModified;

  private ByteBuffer readBuffer = ByteBuffer.allocate(4096);

  private final File file;

  private final TextTailerListener listener;

  private final int interval;

  private long lastPos;

  private final Charset charset;

  private final RandomAccessFile raFile;

  private final ScheduledExecutorService scheduler;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private TextTailer(String file, Charset charset, TextTailerListener listener, int interval,
          ThreadFactory schedulerThreadFactory) {
    if (file == null)
      throw new IllegalArgumentException("file is null");
    if (listener == null)
      throw new IllegalArgumentException("listener is null");

    this.file = new File(file);
    if (!this.file.exists())
      throw new IllegalArgumentException("file does not exist");
    if (!this.file.isFile())
      throw new IllegalArgumentException("file is not a file");
    if (!this.file.canRead())
      throw new IllegalArgumentException("file is not readable");

    this.charset = charset == null ? StandardCharsets.UTF_8 : charset;
    this.listener = listener;
    this.interval = interval;
    this.lastPos = 0;

    try {
      this.raFile = new RandomAccessFile(file, "r");
    }
    catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

    if (schedulerThreadFactory != null)
      scheduler = Executors.newSingleThreadScheduledExecutor(schedulerThreadFactory);
    else
      scheduler = Executors.newSingleThreadScheduledExecutor();
  }


  public void start() {
    // TODO: this harder than it looks, because the file might contains unicode characters
  }


}
