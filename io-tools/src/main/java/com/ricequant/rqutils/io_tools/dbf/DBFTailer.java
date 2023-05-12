package com.ricequant.rqutils.io_tools.dbf;

import com.ricequant.rqutils.io_tools.tailer.BinaryTailer;
import com.ricequant.rqutils.io_tools.tailer.BinaryTailerListener;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * @author kangol
 */
public class DBFTailer {

  private final static int HEADER_LENGTH = 32;

  private final BinaryTailer trailer;

  private final Charset charset;

  private ByteBuffer buffer = ByteBuffer.allocate(4096 * 1024);

  private final Map<String, DBFField> fieldsDef = new LinkedHashMap<>();

  private int rowLength = 0;

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
    this.charset = charset;
    this.buffer.position(0).limit(0);
    this.trailer = new BinaryTailer.Builder().file(file).schedulerThreadFactory(schedulerThreadFactory)
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
                  boolean stopFlag = buffer.get(buffer.position()) == 0x1A;
                  if (stopFlag) {
                    stopPeriodicalScan();
                    break;
                  }
                  DBFRow row = new DBFRow(buffer, fieldsDef);
                  rowListener.accept(row.values());
                }
              }

              @Override
              public void onFileError(IOException e) {

              }

              @Override
              public void onBeforeRead(RandomAccessFile file) throws Exception {
                int numRecords = readNumRecords(file);
                System.out.println("Number of records: " + numRecords);
              }
            }).build();
  }

  public void scan() {
    trailer.scan();
  }

  public void close() {
    trailer.close();
  }

  public void startPeriodicalScan() {
    trailer.startPeriodicalScan();
  }

  public void stopPeriodicalScan() {
    trailer.stopPeriodicalScan();
  }

  private int readNumRecords(RandomAccessFile file) throws IOException {
    file.seek(4);
    byte[] bytes = new byte[4];
    file.read(bytes);
    return bytes[0] & 0xFF | (bytes[1] & 0xFF) << 8 | (bytes[2] & 0xFF) << 16 | (bytes[3] & 0xFF) << 24;
  }

  private int decodeFieldDefs() {
    int offset = HEADER_LENGTH;
    while (true) {
      byte nextByte = buffer.get(offset);
      if (nextByte == 0x0D) {
        break;
      }
      DBFField field = new DBFField(buffer, offset, charset);
      fieldsDef.put(field.name(), field);
      this.rowLength += field.length();
      offset += 32;
    }
    return offset + 1;
  }

}
