package com.ricequant.rqutils.io_tools.dbf;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author kangol
 */
public class DBFField {

  private final String name;

  private final byte type;

  private final int length;

  public static final byte FIELD_TYPE_CHAR = 'C';

  public static final byte FIELD_TYPE_DATE = 'D';

  public static final byte FIELD_TYPE_FLOAT = 'F';

  public static final byte FIELD_TYPE_LOGICAL = 'L';

  public static final byte FIELD_TYPE_MEMO = 'M';

  public static final byte FIELD_TYPE_NUMERIC = 'N';

  DBFField(ByteBuffer fieldDef, int offset, Charset charset) {
    byte[] nameBytes = new byte[11];
    fieldDef.position(offset);
    fieldDef.get(offset, nameBytes);
    name = new String(nameBytes, charset).trim();
    type = fieldDef.get(offset + 11);
    length = fieldDef.get(offset + 16) & 0xff;
  }


  public DBFField(String name, byte type, int length) {
    this.name = name;
    this.type = type;
    this.length = length;
  }

  @Override
  public String toString() {
    return String.format("name: %s, type: %s, length: %s", name, type, length);
  }

  public DBFValue decode(byte[] bytes, int offset, int length, Charset charset) {
    String raw = new String(bytes, offset, length, charset).trim();
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
