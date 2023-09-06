package com.ricequant.rqutils.io_tools.dbf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Kangol
 */
public class DBFWriter {

  private final static int HEADER_LENGTH = 32;

  private final Charset charset;

  private final Map<String, DBFField> fieldsDef = new LinkedHashMap<>();

  private final RandomAccessFile file;

  private final Calendar c = Calendar.getInstance();

  private final int NUM_RECORD_OFFSET = 4;

  private final int INCOMPLETE_TRANS_FLAG_OFFSET = 14;

  private int rowLength = 0;

  DBFWriter(String fileName, List<DBFField> fields, Charset charset, ThreadFactory schedulerThreadFactory)
          throws IOException {
    this.charset = charset;
    File file = new File(fileName);
    if (file.exists()) {
      this.file = new RandomAccessFile(file, "rw");
      this.file.seek(this.file.length());
    }
    else {
      if (file.createNewFile()) {
        this.file = new RandomAccessFile(file, "rw");
        writeHeader();
      }
      else
        throw new IOException("Unable to create file " + fileName);
    }
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
    this.file.writeByte(3);
    int year = c.get(Calendar.YEAR);
    // write modify date
    this.file.writeByte(year - 1900);
    this.file.writeByte(c.get(Calendar.MONTH));
    this.file.writeByte(c.get(Calendar.DATE));

    // write number of records
    this.file.writeInt(0);

    // write header length
    this.file.writeShort(HEADER_LENGTH);

    this.file.writeShort(this.rowLength);

    // reserved
    this.file.writeShort(0);

    // incomplete transaction
    this.file.writeByte(0);

    // encryption=false
    this.file.writeByte(0);

    // Reserved for dBASE for DOS in a multi-user environment
    this.file.write(new byte[12]);

    // Production .mdx file flag; 1 if there is a production .mdx file, 0 if not
    this.file.writeByte(0);

    // Language driver ID
    this.file.writeByte(0);

    // reserved
    this.file.writeShort(0);
  }

  private void writeFieldDef() {

  }

  public void writeRow(DBFValue... values) {

  }
}
