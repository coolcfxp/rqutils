package com.ricequant.rqutils.io_tools.csv;

import com.ricequant.rqutils.io_tools.FileTailer;
import com.ricequant.rqutils.io_tools.tailer.TextTailer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
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

  private final Map<String, String> fieldsMap = new LinkedHashMap<>();

  public CSVTailer(String file, Consumer<Map<String, String>> rowListener, Charset charset,
          ThreadFactory schedulerThreadFactory, String lineSeparator, String fieldSeparator) {
    this.textTailer = new TextTailer.Builder().charset(charset).lineSeparator(lineSeparator)
            .schedulerThreadFactory(schedulerThreadFactory).build(file, row -> {
              String[] fields = row.split(fieldSeparator);
              if (header) {
                fieldNames = fields;
                header = false;
              }
              else {
                fieldsMap.clear();
                for (int i = 0; i < fields.length; i++) {
                  fieldsMap.put(fieldNames[i], fields[i]);
                }
                rowListener.accept(fieldsMap);
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

    public CSVTailer build(String file, Consumer<Map<String, String>> rowListener) {
      return new CSVTailer(file, rowListener, charset, schedulerThreadFactory, lineSeparator, fieldSeparator);
    }
  }
}
