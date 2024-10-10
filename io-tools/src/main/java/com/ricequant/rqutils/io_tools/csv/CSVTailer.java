package com.ricequant.rqutils.io_tools.csv;

import com.ricequant.rqutils.io_tools.FileTailer;
import com.ricequant.rqutils.io_tools.tailer.TextTailer;

import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * @author Kangol
 */
public class CSVTailer implements FileTailer {

  private final TextTailer textTailer;

  private boolean header = true;

  private String[] fieldNames;

  public CSVTailer(String file, Consumer<CSVRow> rowListener, Charset charset, ThreadFactory schedulerThreadFactory,
          String lineSeparator, String fieldSeparator) {
    this.textTailer = new TextTailer.Builder().charset(charset).lineSeparator(lineSeparator)
            .schedulerThreadFactory(schedulerThreadFactory).build(file, row -> {
              String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
              if (header) {
                fieldNames = fields;
                header = false;
              }
              else {
                CSVRow csvRow = new CSVRow();
                for (int i = 0; i < fields.length; i++) {
                  csvRow.put(fieldNames[i], fields[i]);
                }
                rowListener.accept(csvRow);
              }
            });
  }

  @Override
  public void scan() {
    this.textTailer.scan();
  }

  @Override
  public void close() {
    this.textTailer.close();
  }

  @Override
  public RandomAccessFile file() {
    return this.textTailer.file();
  }

  public static class Builder {

    private Charset charset = StandardCharsets.UTF_8;

    private String lineSeparator = System.lineSeparator();

    private String fieldSeparator = ",";

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

    public Builder fieldSeparator(String fieldSeparator) {
      this.fieldSeparator = fieldSeparator;
      return this;
    }

    public CSVTailer build(String file, Consumer<CSVRow> rowListener) {
      return new CSVTailer(file, rowListener, charset, schedulerThreadFactory, lineSeparator, fieldSeparator);
    }
  }
}
