package com.ricequant.rqutils.io_tools.tailer;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author kangol
 */
public interface TextTailerListener {

  void onNewLine(String lineData) throws Throwable;

  void onFileError(IOException e);

  void onBeforeRead(RandomAccessFile file) throws Throwable;
}
