package com.ricequant.rqutils.io_tools.tailer;

import java.io.IOException;

/**
 * @author kangol
 */
public interface BinaryTailerListener {

  void onNewData(byte[] data, int offset, int length);

  void onFileError(IOException e);
}
