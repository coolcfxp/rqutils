package com.ricequant.rqutils.io_tools.csv;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Kangol
 */
public class CSVWriter {
  BufferedWriter writer;
  protected final List<CSVField> fieldsDef;

  private final Calendar c = Calendar.getInstance();
  private String lineSeparator = System.lineSeparator();

  private final FileWriter fileWriter;

  CSVWriter(String fileName, List<CSVField> fields, Charset charset, ThreadFactory schedulerThreadFactory,
          String lineSeparator,
          boolean isWriteHeader)
          throws IOException {
    fieldsDef = fields;
    this.lineSeparator = lineSeparator;

    File file = new File(fileName);
    if (file.exists()) {
      fileWriter =new FileWriter(file, true);
      this.writer = new BufferedWriter(fileWriter);
      ensureNewLine(file);
    }
    else {
      if (file.createNewFile()) {
        fileWriter =new FileWriter(file, true);
        this.writer = new BufferedWriter(fileWriter);
        if (isWriteHeader)
          writeHeader();
      }
      else
        throw new IOException("Unable to create file " + fileName);
    }
  }

  private void ensureNewLine(File file) throws IOException {
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    long length = randomAccessFile.length();
    int numBytesToRead = 2;
    byte[] bytes = new byte[2];
    randomAccessFile.seek(length - numBytesToRead);
    randomAccessFile.read(bytes);
    randomAccessFile.close();
    String lastBytes = new String(bytes);
    if (!lastBytes.equals(lineSeparator)) {
      writer.write(lineSeparator);
      writer.flush();
    }
  }

  public void close() throws IOException {
    fileWriter.close();
    writer.close();
  }

  public static class Builder {

    private Charset charset = StandardCharsets.UTF_8;

    private ThreadFactory schedulerThreadFactory = Executors.defaultThreadFactory();

    private List<CSVField> fields;
    private String lineSeparator = System.lineSeparator();
    private boolean isWriteHeader = true;

    public Builder charset(Charset charset) {
      this.charset = charset;
      return this;
    }

    public Builder isWriteHeader(boolean isWriteHeader) {
      this.isWriteHeader = isWriteHeader;
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
    public Builder fields(CSVField... fields) {
      this.fields = List.of(fields);
      return this;
    }

    public CSVWriter build(String file) throws IOException {
      if (fields == null) {
        throw new IllegalArgumentException("fields must be set");
      }

      return new CSVWriter(file, fields, charset, schedulerThreadFactory, lineSeparator, isWriteHeader);
    }
  }

  private void writeHeader() throws IOException {
    List<String> fieldNames = fieldsDef.stream().map(CSVField::name).toList();
    writer.write(String.join(",", fieldNames) + lineSeparator);
    writer.flush();
  }

  public void writeRow(CSVValue... values) throws IOException {
    if (values.length != fieldsDef.size())
      throw new RuntimeException("Number of values does not match number of fields");
    Iterator<CSVField> fieldsDefIter = fieldsDef.iterator();
    List<String> formattedList = new ArrayList<>();

    for (CSVValue v : values) {
      CSVField f = fieldsDefIter.next();
      formattedList.add(f.encode(v));
    }

    String row = String.join(",", formattedList) + lineSeparator;
    writer.write(row);
    writer.flush();
  }
}
