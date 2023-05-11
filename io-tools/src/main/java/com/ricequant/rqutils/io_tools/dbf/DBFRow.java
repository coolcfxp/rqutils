package com.ricequant.rqutils.io_tools.dbf;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author kangol
 */
public class DBFRow {

  private final Map<String, DBFValue> values = new LinkedHashMap<>();

  DBFRow(ByteBuffer buffer, Map<String, DBFField> fieldsDef) {
    byte[] bytes = new byte[1024];
    buffer.get(); // skip deleted flag
    for (DBFField field : fieldsDef.values()) {
      if (field.length() > bytes.length) {
        bytes = new byte[field.length()];
      }
      buffer.get(bytes, 0, field.length());
      buffer.position(buffer.position() + field.length());
      values.put(field.name(), field.decode(bytes, 0, field.length()));
    }
  }

  public DBFValue get(String name) {
    return values.get(name);
  }

  public Map<String, DBFValue> values() {
    return values;
  }
}
