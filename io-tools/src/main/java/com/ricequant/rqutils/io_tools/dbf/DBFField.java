package com.ricequant.rqutils.io_tools.dbf;

import java.nio.ByteBuffer;

/**
 * @author kangol
 */
public class DBFField {

  private final String name;

  private final byte type;

  private final int length;

  DBFField(ByteBuffer fieldDef, int offset) {
    byte[] nameBytes = new byte[11];
    fieldDef.position(offset);
    fieldDef.get(offset, nameBytes);
    name = new String(nameBytes).trim();
    type = fieldDef.get(offset + 11);
    length = fieldDef.get(offset + 16);
  }

  @Override
  public String toString() {
    return String.format("name: %s, type: %s, length: %s", name, type, length);
  }

  public DBFValue decode(byte[] bytes, int offset, int length) {
    String raw = new String(bytes, offset, length).trim();
    if (type == 'C') {
      // String type
      return new DBFValue(raw);
    }
    else if (type == 'N' || type == 'F') {
      // Number type
      return new DBFValue(Double.parseDouble(raw));
    }
    else if (type == 'D') {
      // Date type
      return new DBFValue(Integer.parseInt(raw));
    }
    else if (type == 'L') {
      // Boolean type
      return new DBFValue(raw.equals("T") || raw.equals("Y") || raw.equals("1") || raw.equals("t") || raw.equals("y"));
    }
    else if (type == 'M') {
      // Memo type
      return new DBFValue(raw);
    }

    return new DBFValue(raw);
  }

  public String name() {
    return name;
  }

  public byte type() {
    return type;
  }

  public int length() {
    return length;
  }
}
