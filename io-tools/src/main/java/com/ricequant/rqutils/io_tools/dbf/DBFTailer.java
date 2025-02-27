package com.ricequant.rqutils.io_tools.dbf;

import com.ricequant.rqutils.io_tools.FileTailer;
import com.ricequant.rqutils.io_tools.tailer.BinaryTailer;
import com.ricequant.rqutils.io_tools.tailer.BinaryTailerListener;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * @author kangol
 */
public class DBFTailer extends DBFCodec implements FileTailer {

  private final BinaryTailer tailer;

  public static class Builder {

    private Charset charset = StandardCharsets.UTF_8;

    private int reopenInterval = 0;

    private ThreadFactory schedulerThreadFactory = Executors.defaultThreadFactory();

    private boolean compareFileContentWhenReopen;

    private File comparisonFailedFileContentBackup;

    private Consumer<Map<String, DBFField>> fieldModifier;

    public Builder charset(Charset charset) {
      this.charset = charset;
      return this;
    }

    public Builder reopenInterval(int reopenInterval) {
      this.reopenInterval = reopenInterval;
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

    public Builder fieldModifier(Consumer<Map<String, DBFField>> fieldModifier) {
      this.fieldModifier = fieldModifier;
      return this;
    }

    public DBFTailer build(String file, Consumer<DBFRow> rowListener) {
      return new DBFTailer(file, rowListener, charset, schedulerThreadFactory, reopenInterval,
              compareFileContentWhenReopen, comparisonFailedFileContentBackup, fieldModifier);
    }
  }

  private DBFTailer(String file, Consumer<DBFRow> rowListener, Charset charset, ThreadFactory schedulerThreadFactory,
          int reopenInterval, boolean compareFileContentWhenReopen, File comparisonFailedFileContentBackup,
          Consumer<Map<String, DBFField>> fieldModifier) {
    super(file, charset, fieldModifier);
    this.buffer.position(0).limit(0);
    this.tailer = new BinaryTailer.Builder().reopenInterval(reopenInterval).file(file)
            .compareFileContentWhenReopen(compareFileContentWhenReopen)
            .comparisonFailedFileContentBackup(comparisonFailedFileContentBackup)
            .schedulerThreadFactory(schedulerThreadFactory).listener(new BinaryTailerListener() {
              @Override
              public void onNewData(byte[] data, int offset, int length) {
                buffer.compact();
                if (buffer.remaining() < length) {
                  ByteBuffer buf = ByteBuffer.allocate(buffer.capacity() * 2);
                  System.out.println("Allocating buffer of size " + buf.capacity());
                  buf.put(buffer);
                  buffer = buf;
                }

                buffer.put(data, offset, length);
                buffer.flip();

                if (rowLength == 0) {
                  int bodyOffset = decodeFieldDefs();
                  buffer.position(bodyOffset);
                }

                while (buffer.remaining() >= rowLength) {
                  int rowBegin = buffer.position();
                  byte deletionFlag = buffer.get();
                  if (deletionFlag == 0x2A) {
                    System.out.println(file + ": delete flag detected: " + deletionFlag);
                  }
                  DBFRow row = new DBFRow(buffer, fieldsDef, charset);
                  buffer.position(rowBegin + rowLength);
                  try {
                    rowListener.accept(row);
                  }
                  catch (Throwable e) {
                    e.printStackTrace();
                  }
                }
              }

              @Override
              public void onFileError(IOException e) {
                e.printStackTrace();
              }

              @Override
              public void onBeforeRead(RandomAccessFile file) throws Exception {
                int numRecords = readNumRecords(file);
                /*          System.out.println(DBFTailer.this.fileName + ": Number of records: " + numRecords);*/
              }
            }).build();
  }

  @Override
  public DBFTailer scan() {
    tailer.scan();
    return this;
  }

  @Override
  public DBFTailer close() {
    tailer.close();
    return this;
  }

  public void startPeriodicalScan() {
    tailer.startPeriodicalScan();
  }

  public void stopPeriodicalScan() {
    tailer.stopPeriodicalScan();
  }

}
