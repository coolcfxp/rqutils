package com.ricequant.rqutils.network;

import java.nio.ByteBuffer;

/**
 * @author liche
 */
public interface ServiceProcessor {

  int id();

  String name();

  void onConnected(long sessionID, MixedServiceBinarySender sender);

  void process(long sessionID, ByteBuffer toDecode);

  void onDisconnected(long sessionID);
}
