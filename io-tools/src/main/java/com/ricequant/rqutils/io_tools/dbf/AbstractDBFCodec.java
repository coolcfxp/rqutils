package com.ricequant.rqutils.io_tools.dbf;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @author Kangol
 */
public abstract class AbstractDBFCodec {

  public final static int HEADER_LENGTH = 32;

  private static final int NUM_RECORD_OFFSET = 4;

  protected final Map<String, DBFField> fieldsDef = new LinkedHashMap<>();

  protected int rowLength = 0;

  protected final String fileName;

  protected final Charset charset;

  protected byte[] padding;

  protected ByteBuffer buffer = ByteBuffer.allocate(4096 * 1024).order(ByteOrder.LITTLE_ENDIAN);

  AbstractDBFCodec(String fileName, Charset charset) {
    this.fileName = fileName;
    this.charset = charset;
  }

  protected int decodeFieldDefs() {
    this.fieldsDef.clear();

    short headerLen = buffer.getShort(8);

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

    this.rowLength += 1;

    int headerDefinedRowLength = (buffer.get(11) & 0xFF) << 8 | buffer.get(10) & 0xFF;
    if (headerDefinedRowLength != this.rowLength) {
      System.out.println(
              fileName + ": Header defined row length " + headerDefinedRowLength + " is not equal to calculated row "
                      + "length " + this.rowLength + ", padding with 0x20");
      int numPadding = headerDefinedRowLength - this.rowLength;
      this.padding = new byte[numPadding];
      Arrays.fill(this.padding, (byte) 0x20);
      this.rowLength = headerDefinedRowLength;
    }
    else
      this.padding = new byte[0];
    offset = offset + 1;
    if (offset < headerLen)
      return headerLen;
    return offset;
  }

  protected int readNumRecords(RandomAccessFile file) throws IOException {
    long pointer = file.getFilePointer();
    file.seek(NUM_RECORD_OFFSET);
    byte[] bytes = new byte[4];
    file.read(bytes);
    file.seek(pointer);
    return bytes[0] & 0xFF | (bytes[1] & 0xFF) << 8 | (bytes[2] & 0xFF) << 16 | (bytes[3] & 0xFF) << 24;
  }

  public List<DBFField> fields() {
    return new ArrayList<>(fieldsDef.values());
  }
}
