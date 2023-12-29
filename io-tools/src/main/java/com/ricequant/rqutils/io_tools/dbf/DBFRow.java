package com.ricequant.rqutils.io_tools.dbf;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author kangol
 */
public class DBFRow {

  private final Map<String, DBFValue> values = new LinkedHashMap<>();

  DBFRow(ByteBuffer buffer, Map<String, DBFField> fieldsDef, Charset charset) {
    byte[] bytes = new byte[1024];
    for (DBFField field : fieldsDef.values()) {
      if (field.length() > bytes.length) {
        bytes = new byte[field.length()];
      }
      buffer.get(bytes, 0, field.length());
      values.put(field.name(), field.decode(bytes, 0, field.length(), charset));
    }
  }

  public DBFRow() {

  }

  public DBFRow put(String name, DBFValue value) {
    values.put(name, value);
    return this;
  }

  public DBFValue get(String name) {
    return values.get(name);
  }

  public Map<String, DBFValue> values() {
    return values;
  }

}
