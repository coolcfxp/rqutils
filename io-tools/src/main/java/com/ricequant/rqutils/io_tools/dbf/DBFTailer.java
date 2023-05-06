package com.ricequant.rqutils.io_tools.dbf;

import com.ricequant.rqutils.io_tools.tailer.BinaryTailer;
import com.ricequant.rqutils.io_tools.tailer.BinaryTailerListener;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author kangol
 */
public class DBFTailer {

  private final static int HEADER_LENGTH = 32;

  private final BinaryTailer trailer;

  private ByteBuffer buffer = ByteBuffer.allocate(4096 * 1024);

  private final Map<String, DBFField> fieldsDef = new LinkedHashMap<>();

  private int rowLength = 0;

  public DBFTailer(String file, Consumer<Map<String, DBFValue>> rowListener) throws IOException {
    this.trailer = new BinaryTailer.Builder().file(file).listener(new BinaryTailerListener() {
      @Override
      public void onNewData(byte[] data, int offset, int length) {
        if (buffer.remaining() < length) {
          buffer.flip();
          ByteBuffer buf = ByteBuffer.allocate(buffer.capacity() * 2);
          System.out.println("Allocating buffer of size " + buf.capacity());
          buf.put(buffer);
          buffer = buf;
        }

        buffer.put(data, offset, length);

        if (rowLength == 0) {
          int bodyOffset = decodeFieldDefs();
          buffer.limit(buffer.capacity()).position(bodyOffset);
        }

        boolean stopFlag = buffer.get(buffer.position()) == 0x1A;
        while (!stopFlag && buffer.remaining() >= rowLength) {
          DBFRow row = new DBFRow(buffer, fieldsDef);
          rowListener.accept(row.values());
        }

        if (stopFlag) {
          stop();
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

  public void start() {
    trailer.start();
  }

  public void stop() {
    trailer.stop();
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
      DBFField field = new DBFField(buffer, offset);
      fieldsDef.put(field.name(), field);
      this.rowLength += field.length();
      offset += 32;
    }
    return offset + 1;
  }

}
