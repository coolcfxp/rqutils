package com.ricequant.rqutils.io_tools.dbf;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

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

  private final Charset charset;

  private boolean isLong;

  DBFField(ByteBuffer fieldDef, int offset, Charset charset) {
    byte[] nameBytes = new byte[11];
    fieldDef.position(offset);
    fieldDef.get(offset, nameBytes);
    name = new String(nameBytes, charset).trim();
    type = fieldDef.get(offset + 11);
    length = fieldDef.get(offset + 16) & 0xff;
    this.charset = charset;
  }

  public DBFField setNumericToLong() {
    this.isLong = true;
    return this;
  }

  public DBFField(String name, byte type, int dataLength, Charset charset) {
    this.name = name;
    this.type = type;
    this.length = dataLength;
    this.charset = charset;
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
      if (raw.trim().isEmpty())
        return new DBFValue(isLong ? 0L : 0.0);
      return isLong ? new DBFValue(Long.parseLong(raw)) : new DBFValue(Double.parseDouble(raw));
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

  public void encode(DBFValue value, byte[] target, int offset) {
    if (value == null) {
      Arrays.fill(target, offset, offset + length, (byte) 0x20);
      return;
    }
    if (type == FIELD_TYPE_CHAR || type == FIELD_TYPE_MEMO || type == FIELD_TYPE_DATE) {
      byte[] bytes = value.stringValue().getBytes(charset);
      if (bytes.length > target.length - offset)
        throw new IllegalArgumentException("target buffer is too small");
      if (bytes.length > length)
        throw new IllegalArgumentException(
                "data=" + value + ", data is too long, fieldWidth=" + length + ", " + "dataWidth=" + bytes.length);

      System.arraycopy(bytes, 0, target, offset, bytes.length);
      Arrays.fill(target, offset + bytes.length, offset + length, (byte) 0x20);
    }
    else if (type == FIELD_TYPE_NUMERIC || type == FIELD_TYPE_FLOAT) {
      byte[] bytes;
      if (value.isDouble())
        bytes = String.valueOf(value.doubleValue()).getBytes(charset);
      else
        bytes = String.valueOf(value.longValue()).getBytes(charset);

      if (bytes.length > target.length - offset)
        throw new IllegalArgumentException("target buffer is too small");
      System.arraycopy(bytes, 0, target, offset + length - bytes.length, bytes.length);
      Arrays.fill(target, offset, offset + length - bytes.length, (byte) 0x20);
    }
    else if (type == FIELD_TYPE_LOGICAL) {
      if (target.length - offset < 1)
        throw new IllegalArgumentException("target buffer is too small");
      target[offset + 1] = value.booleanValue() ? (byte) 'T' : (byte) 'F';
      Arrays.fill(target, offset + 1, offset + length, (byte) 0x20);
    }
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
