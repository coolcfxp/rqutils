package com.ricequant.rqutils.io_tools.tailer;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author kangol
 */
public interface BinaryTailerListener {

  void onNewData(byte[] data, int offset, int length) throws Exception;

  void onFileError(IOException e);

  void onBeforeRead(RandomAccessFile file) throws Exception;
}
