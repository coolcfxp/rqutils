package com.ricequant.rqutils.io_tools.tailer;

import com.ricequant.rqutils.io_tools.FileTailer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * @author Kangol
 */
public class TextTailer implements FileTailer {

  private ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

  private final BinaryTailer tailer;

  public static class Builder {

    private Charset charset = StandardCharsets.UTF_8;

    private String lineSeparator = System.lineSeparator();

    private ThreadFactory schedulerThreadFactory = Executors.defaultThreadFactory();

    public Builder charset(Charset charset) {
      this.charset = charset;
      return this;
    }

    public Builder schedulerThreadFactory(ThreadFactory schedulerThreadFactory) {
      this.schedulerThreadFactory = schedulerThreadFactory;
      return this;
    }

    public Builder lineSeparator(String lineSeparator) {
      this.lineSeparator = lineSeparator;
      return this;
    }

    public TextTailer build(String file, Consumer<String> rowListener) {
      return new TextTailer(file, rowListener, charset, schedulerThreadFactory, lineSeparator);
    }
  }

  private TextTailer(String file, Consumer<String> rowListener, Charset charset, ThreadFactory schedulerThreadFactory,
          String lineSeparator) {
    this.tailer = new BinaryTailer.Builder().file(file).schedulerThreadFactory(schedulerThreadFactory)
            .listener(new BinaryTailerListener() {
              @Override
              public void onNewData(byte[] data, int offset, int length) throws Throwable {
                buffer.compact();
                if (buffer.remaining() < length) {
                  ByteBuffer buf = ByteBuffer.allocate(buffer.capacity() * 2);
                  System.out.println("Allocating buffer of size " + buf.capacity());
                  buf.put(buffer);
                  buffer = buf;
                }
                buffer.put(data, offset, length);
                buffer.flip();

                int start = buffer.position();
                int limit = buffer.limit();
                int end = start;

                while (end < limit) {
                  boolean found = true;
                  for (int i = 0; i < lineSeparator.length(); i++) {
                    if (end + i >= limit) {
                      found = false;
                      break;
                    }
                    if (buffer.get(end + i) != lineSeparator.charAt(i)) {
                      found = false;
                      break;
                    }
                  }
                  if (found) {
                    end += lineSeparator.length();
                    String line = new String(buffer.array(), start, end - start - lineSeparator.length(), charset);
                    rowListener.accept(line);
                    buffer.position(end);
                    start = end;
                  }
                  else {
                    end++;
                  }
                }
              }

              @Override
              public void onFileError(IOException e) {
                e.printStackTrace();
              }

              @Override
              public void onBeforeRead(RandomAccessFile file) throws Throwable {

              }
            }).build();
  }

  @Override
  public void scan() {
    this.tailer.scan();
  }

  @Override
  public void close() {
    this.tailer.close();
  }
}
