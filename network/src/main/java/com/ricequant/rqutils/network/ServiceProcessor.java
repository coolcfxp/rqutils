package com.ricequant.rqutils.network;

import java.nio.ByteBuffer;

/**
 * @author liche
 */
public interface ServiceProcessor {

  int id();

  String name();

  void onConnected(long connectionID, MixedServiceBinarySender sender);

  void process(long connectionID, ByteBuffer toDecode);

  void onDisconnected(long sessionID);
}
