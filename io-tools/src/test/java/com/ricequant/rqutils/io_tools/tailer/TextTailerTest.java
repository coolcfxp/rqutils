package com.ricequant.rqutils.io_tools.tailer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * @author Kangol
 */
@Disabled
public class TextTailerTest {

  @Test
  void testRead() {
    TextTailer tailer = new TextTailer.Builder().build("src/test/resources/csvtest.csv", row -> {
      System.out.println(row);
    });
    tailer.scan();
    tailer.scan();
  }
}
