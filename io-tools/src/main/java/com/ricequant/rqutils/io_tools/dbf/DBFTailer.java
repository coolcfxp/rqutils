package com.ricequant.rqutils.io_tools.dbf;

import com.ricequant.rqutils.io_tools.FileTailer;
import com.ricequant.rqutils.io_tools.tailer.BinaryTailer;
import com.ricequant.rqutils.io_tools.tailer.BinaryTailerListener;

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
public class DBFTailer extends AbstractDBFCodec implements FileTailer {

  private final BinaryTailer tailer;

  public static class Builder {

    private Charset charset = StandardCharsets.UTF_8;

    private ThreadFactory schedulerThreadFactory = Executors.defaultThreadFactory();

    public Builder charset(Charset charset) {
      this.charset = charset;
      return this;
    }

    public Builder schedulerThreadFactory(ThreadFactory schedulerThreadFactory) {
      this.schedulerThreadFactory = schedulerThreadFactory;
      return this;
    }

    public DBFTailer build(String file, Consumer<Map<String, DBFValue>> rowListener) {
      return new DBFTailer(file, rowListener, charset, schedulerThreadFactory);
    }
  }

  private DBFTailer(String file, Consumer<Map<String, DBFValue>> rowListener, Charset charset,
          ThreadFactory schedulerThreadFactory) {
    super(file, charset);
    this.buffer.position(0).limit(0);
    this.tailer = new BinaryTailer.Builder().file(file).schedulerThreadFactory(schedulerThreadFactory)
            .listener(new BinaryTailerListener() {
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
                    rowListener.accept(row.values());
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
  public void scan() {
    tailer.scan();
  }

  @Override
  public void close() {
    tailer.close();
  }

  public void startPeriodicalScan() {
    tailer.startPeriodicalScan();
  }

  public void stopPeriodicalScan() {
    tailer.stopPeriodicalScan();
  }

}
