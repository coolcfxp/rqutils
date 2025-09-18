package com.ricequant.rqutils.io_tools.dbf;

import com.ricequant.rqutils.io_tools.TableRowSource;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author kangol
 */
public class DBFRow implements TableRowSource {

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

  @Override
  public String getString(String... names) {
    DBFValue value = get(names);
    if (value == null)
      return null;
    return value.convertToString().stringValue();
  }

  @Override
  public DBFValue get(String... names) {
    if (names.length == 0)
      return null;

    if (names.length == 1)
      return values.get(names[0]);

    DBFValue emptyStringValueCandidate = null;
    for (String name : names) {
      DBFValue ret = values.get(name);
      if (ret != null) {
        if (ret.isString() && (ret.stringValue() == null || ret.stringValue().trim().isEmpty())) {
          emptyStringValueCandidate = ret;
          continue;
        }
        return ret;
      }
    }

    return emptyStringValueCandidate;
  }

  public boolean containsKey(String... names) {
    if (names.length == 0)
      return false;

    if (names.length == 1)
      return values.containsKey(names[0]);

    for (String name : names) {
      if (values.containsKey(name))
        return true;
    }
    return false;
  }

  public Map<String, DBFValue> values() {
    return values;
  }

  @Override
  public String toString() {
    return values.toString();
  }
}
