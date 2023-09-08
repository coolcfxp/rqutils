package com.ricequant.rqutils.io_tools.tailer;

import com.ricequant.rqutils.io_tools.dbf.DBFField;
import com.ricequant.rqutils.io_tools.dbf.DBFTailer;
import com.ricequant.rqutils.io_tools.dbf.DBFValue;
import com.ricequant.rqutils.io_tools.dbf.DBFWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Kangol
 */
@Disabled
class DBFCodecTest {

  private DBFWriter writer;

  private File testFile;

  @BeforeEach
  void setUp() throws Exception {
    File tempDir = createTempDirectory("test");
    tempDir.deleteOnExit();
    this.testFile = Path.of(tempDir.getAbsolutePath(), "test.dbf").toFile();
    testFile.deleteOnExit();
    this.writer = new DBFWriter.Builder().fields(
                    new DBFField("Field1", DBFField.FIELD_TYPE_CHAR,
                            "123456789".getBytes(StandardCharsets.UTF_8).length),
                    new DBFField("Field2", DBFField.FIELD_TYPE_CHAR,
                            "12345".getBytes(StandardCharsets.UTF_8).length))
            .build(testFile.getAbsolutePath());

    System.out.println(testFile.getAbsolutePath());
  }

  @Test
  void testWriteThenRead() {
    this.writer.writeRow(new DBFValue("12345678"), new DBFValue("abcd"));
    this.writer.writeRow(new DBFValue("1234568"), new DBFValue("abcde"));

    int[] count = new int[]{0};
    DBFTailer tailer = new DBFTailer.Builder().build(testFile.getAbsolutePath(), row -> {
      if (count[0] == 0) {
        assertEquals("12345678", row.get("Field1").stringValue());
        assertEquals("abcd", row.get("Field2").stringValue());
      }
      else if (count[0] == 1) {
        assertEquals("1234568", row.get("Field1").stringValue());
        assertEquals("abcde", row.get("Field2").stringValue());
      }
      count[0]++;
    });

    tailer.scan();

    assertEquals(2, count[0]);
    tailer.close();
  }

  public static File createTempDirectory(String prefix) throws IOException {
    String tempDir = System.getProperty("java.io.tmpdir");
    File generatedDir = new File(tempDir, prefix + "_" + System.currentTimeMillis());

    if (generatedDir.exists() && generatedDir.isDirectory() && generatedDir.canWrite())
      return generatedDir;

    if (!generatedDir.mkdir())
      throw new IOException("Failed to create temp directory " + generatedDir.getName());

    return generatedDir;
  }

}