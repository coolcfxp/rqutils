package com.ricequant.rqutils.io_tools;

import java.io.RandomAccessFile;

/**
 * @author Kangol
 */
public interface FileTailer {

  void scan();

  void close();

  RandomAccessFile file();
}
