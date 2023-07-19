package com.ricequant.rqutils.io_tools.tailer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Kangol
 */
@Disabled
class BinaryTailerTest {

  private RandomAccessFile rafile;

  @BeforeEach
  void setUp() throws FileNotFoundException {
    this.rafile = new RandomAccessFile(new File("src/test/resources/test.txt"), "r");
  }

  @Test
  void testFilePosition() throws IOException {
    byte[] buffer = new byte[4];
    this.rafile.read(buffer, 0, 4);

    System.out.println(new String(buffer));

    long pos = this.rafile.getFilePointer();

    this.rafile.seek(pos);

    System.out.println(pos);

    this.rafile.read(buffer, 0, 4);
    System.out.println(new String(buffer));

    int read = this.rafile.read(buffer, 0, 4);
    System.out.println(read);
  }
}