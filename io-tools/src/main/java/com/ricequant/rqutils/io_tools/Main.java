package com.ricequant.rqutils.io_tools;

import com.ricequant.rqutils.io_tools.dbf.DBFTailer;
import com.ricequant.rqutils.io_tools.dbf.DBFValue;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ${USER}
 */
public class Main {

  public static String toString(Map<String, DBFValue> values) {
    StringBuilder str = new StringBuilder();
    for (Map.Entry<String, DBFValue> entry : values.entrySet()) {
      str.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
    }

    return str.toString();
  }

  public static void main(String[] args) {
    System.out.println("Hello world!");
    String command = args[0];

    if (command.equals("scan_dbf")) {
      AtomicLong atomicLong = new AtomicLong();
      String filePath = args[1];
      DBFTailer tailer = new DBFTailer.Builder().charset(Charset.forName("GBK")).build(filePath,
              row -> {
                if (row.get("ACCOUNT").stringValue().equals("41900049176")) {
                  atomicLong.incrementAndGet();
                }
              });
      tailer.scan();

      System.out.println("Cnt: " + atomicLong.get());
    }

  }
}