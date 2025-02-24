package com.ricequant.rqutils.io_tools.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.util.function.Consumer;

/**
 * @author kangol
 */
public class CSVReader {

  public static void parse(File csv, Charset charset, Consumer<CSVRow> fieldsProcessor) {
    try (BufferedReader br = new BufferedReader(new FileReader(csv, charset))) {
      String line;
      boolean header = true;
      String[] fieldNames = new String[0];
      while ((line = br.readLine()) != null) {
        String[] fields = line.split("[,|](?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (header) {
          fieldNames = fields;
          header = false;
        }
        else {
          CSVRow csvRow = new CSVRow();
          for (int i = 0; i < fields.length; i++) {
            csvRow.put(fieldNames[i], fields[i]);
          }
          fieldsProcessor.accept(csvRow);
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
