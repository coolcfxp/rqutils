package com.ricequant.rqutils.io_tools.dbf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Kangol
 */
public class DBFWriter extends AbstractDBFCodec {

  private final static int HEADER_LENGTH = 32;

  private final static int NUM_RECORD_OFFSET = 4;

  private final FileChannel channel;

  private int numRecords = 0;

  private final RandomAccessFile file;

  private final Calendar c = Calendar.getInstance();

  private final ByteBuffer buffer = ByteBuffer.allocate(4096 * 1024).order(ByteOrder.LITTLE_ENDIAN);

  private final int INCOMPLETE_TRANS_FLAG_OFFSET = 14;

  private final ThreadFactory schedulerThreadFactory;

  DBFWriter(String fileName, List<DBFField> fields, Charset charset, ThreadFactory schedulerThreadFactory)
          throws IOException {
    super(fileName, charset);

    for (var field : fields) {
      fieldsDef.put(field.name(), field);
    }

    File file = new File(fileName);
    if (file.exists()) {
      this.file = new RandomAccessFile(file, "rw");
      this.channel = this.file.getChannel();
      decodeFieldDefs();
      numRecords = readNumRecords(this.file);
      this.file.seek(this.file.length());
    }
    else {
      if (file.createNewFile()) {
        this.file = new RandomAccessFile(file, "rw");
        this.channel = this.file.getChannel();
        computeRowLength();
        writeHeader();
      }
      else
        throw new IOException("Unable to create file " + fileName);
    }


    this.schedulerThreadFactory = schedulerThreadFactory;
  }

  public static class Builder {

    private Charset charset = StandardCharsets.UTF_8;

    private ThreadFactory schedulerThreadFactory = Executors.defaultThreadFactory();

    private List<DBFField> fields;

    public Builder charset(Charset charset) {
      this.charset = charset;
      return this;
    }

    public Builder schedulerThreadFactory(ThreadFactory schedulerThreadFactory) {
      this.schedulerThreadFactory = schedulerThreadFactory;
      return this;
    }

    public Builder fields(DBFField... fields) {
      this.fields = List.of(fields);
      return this;
    }

    public DBFWriter build(String file) throws IOException {
      if (fields == null) {
        throw new IllegalArgumentException("fields must be set");
      }

      return new DBFWriter(file, fields, charset, schedulerThreadFactory);
    }
  }

  private void writeHeader() throws IOException {
    buffer.put((byte) 3);
    int year = c.get(Calendar.YEAR);
    // write modify date
    buffer.put(new byte[]{(byte) (year - 1900), (byte) c.get(Calendar.MONTH), (byte) c.get(Calendar.DATE)});

    // write number of records
    buffer.putInt(0);

    // write header length
    buffer.putShort((short) HEADER_LENGTH);

    buffer.putShort((short) this.rowLength);

    // reserved
    buffer.putShort((short) 0);

    // incomplete transaction
    buffer.put((byte) 0);

    // encryption=false
    buffer.put((byte) 0);

    // Reserved for dBASE for DOS in a multi-user environment
    buffer.put(new byte[12]);

    // Production .mdx file flag; 1 if there is a production .mdx file, 0 if not
    buffer.put((byte) 0);

    // Language driver ID
    buffer.put((byte) 0);

    // reserved
    buffer.putShort((short) 0);

    this.channel.write(buffer.flip());
    buffer.clear();

    writeFieldDef();
  }

  private void computeRowLength() {
    rowLength = 0;
    for (DBFField f : this.fieldsDef.values()) {
      rowLength += f.length() + 1;
    }
    int numPadding = (rowLength % 2 == 1 ? 1 : 0);
    this.padding = new byte[numPadding];
    Arrays.fill(this.padding, (byte) 0x20);
    rowLength += numPadding;
  }

  private void writeFieldDef() throws IOException {
    int i = 0;
    for (DBFField f : this.fieldsDef.values()) {
      buffer.clear();
      byte[] nameBytes = f.name().getBytes(charset);
      if (nameBytes.length > 11)
        throw new RuntimeException("Field name is too long: " + f.name());
      buffer.put(nameBytes);
      if (nameBytes.length < 11)
        buffer.put(new byte[11 - nameBytes.length]);
      buffer.put(f.type());
      buffer.put(new byte[4]);
      buffer.put((byte) f.length());

      // field decimal count
      buffer.put((byte) (i++));
      // work area id (2)
      // example (1)
      // reserved (10)
      // MDX field flag (1)
      buffer.put(new byte[14]);
      this.channel.write(buffer.flip());
    }
    buffer.clear();
    buffer.put((byte) 0x0D);
    buffer.put((byte) 0);
    this.channel.write(buffer.flip());
  }

  public void writeRow(DBFValue... values) {
    if (values.length != fieldsDef.size())
      throw new RuntimeException("Number of values does not match number of fields");

    buffer.clear();
    Iterator<DBFField> fieldsDefIter = fieldsDef.values().iterator();
    try {
      this.buffer.put((byte) 0x20);
      for (DBFValue v : values) {
        int fieldLength = fieldsDefIter.next().length();
        if (v.isString())
          writePaddedBytes(v.stringValue(), fieldLength);
        else if (v.isDouble())
          writePaddedBytes(String.valueOf(v.doubleValue()), fieldLength);
        else if (v.isBoolean()) {
          writePaddedBytes(v.booleanValue() ? "T" : "F", fieldLength);
        }
        else {
          throw new RuntimeException("Unknown type: " + v);
        }
      }
      if (this.padding.length > 0)
        this.buffer.put(this.padding);
      this.channel.write(buffer.flip());

      numRecords++;
      long pointer = this.file.getFilePointer();
      this.file.seek(NUM_RECORD_OFFSET);
      writeIntLittleEndian(this.file, numRecords);
      this.file.seek(pointer);

    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writePaddedBytes(String dataString, int fieldLength) throws IOException {
    byte[] data = dataString.getBytes(charset);
    this.buffer.put(data);
    if (data.length < fieldLength) {
      byte[] padded = new byte[fieldLength - data.length];
      Arrays.fill(padded, (byte) 0x20);
      this.buffer.put(padded);
    }
  }

  private static void writeIntLittleEndian(RandomAccessFile file, int value) throws IOException {
    int a = value & 0xFF;
    int b = (value >> 8) & 0xFF;
    int c = (value >> 16) & 0xFF;
    int d = (value >> 24) & 0xFF;
    file.writeByte(a);
    file.writeByte(b);
    file.writeByte(c);
    file.writeByte(d);
  }
}
