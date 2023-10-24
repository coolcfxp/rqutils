package com.ricequant.rqutils.io_tools.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

  private int numRecords = 0;

  private final Calendar c = Calendar.getInstance();

  CSVWriter(String fileName, List<CSVField> fields, Charset charset, ThreadFactory schedulerThreadFactory)
          throws IOException {
    fieldsDef = fields;

    File file = new File(fileName);
    if (file.exists()) {
      this.writer = new BufferedWriter(new FileWriter(file, true));
    }
    else {
      if (file.createNewFile()) {
        this.writer = new BufferedWriter(new FileWriter(file, true));
        writeHeader();
      }
      else
        throw new IOException("Unable to create file " + fileName);
    }
  }

  public static class Builder {

    private Charset charset = StandardCharsets.UTF_8;

    private ThreadFactory schedulerThreadFactory = Executors.defaultThreadFactory();

    private List<CSVField> fields;

    public Builder charset(Charset charset) {
      this.charset = charset;
      return this;
    }

    public Builder schedulerThreadFactory(ThreadFactory schedulerThreadFactory) {
      this.schedulerThreadFactory = schedulerThreadFactory;
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

      return new CSVWriter(file, fields, charset, schedulerThreadFactory);
    }
  }

  private void writeHeader() throws IOException {
    List<String> fieldNames = fieldsDef.stream().map(CSVField::name).toList();
    writer.write(String.join(",", fieldNames));
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

    String row = String.join(",", formattedList);
    writer.write(row);
    writer.flush();
  }
}
