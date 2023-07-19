package com.ricequant.rqutils.io_tools.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.function.Consumer;

/**
 * @author kangol
 */
public class CSVReader {

  public final static String COMMA_DELIMITER = ",";

  public static void parse(File csv, boolean skipHeader, String separator, Consumer<String[]> fieldsProcessor) {
    try (BufferedReader br = new BufferedReader(new FileReader(csv))) {
      String line;
      int count = 0;
      while ((line = br.readLine()) != null) {
        if (count++ == 0 && skipHeader)
          continue;

        String[] values = line.split(separator);
        fieldsProcessor.accept(values);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
