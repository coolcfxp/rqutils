package com.ricequant.rqutils.io_tools.dbf;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kangol
 */
public class DBFValue {

  private final String stringValue;

  private final double doubleValue;

  private final boolean booleanValue;

  private final long longValue;

  private boolean isLong = false;

  private boolean isString = false;

  private boolean isDouble = false;

  private boolean isBoolean = false;

  public DBFValue(String stringValue) {
    this.stringValue = stringValue;
    this.doubleValue = 0;
    this.booleanValue = false;
    this.longValue = 0;
    isString = true;
  }

  public DBFValue(double doubleValue) {
    this.stringValue = null;
    this.doubleValue = doubleValue;
    this.booleanValue = false;
    this.longValue = 0;
    isDouble = true;
  }

  public DBFValue(boolean booleanValue) {
    this.stringValue = null;
    this.doubleValue = 0;
    this.booleanValue = booleanValue;
    this.longValue = 0;
    isBoolean = true;
  }

  public DBFValue(long longValue) {
    this.stringValue = null;
    this.doubleValue = 0;
    this.booleanValue = false;
    this.longValue = longValue;
    isLong = true;
  }

  public String stringValue() {
    if (!isString)
      throw new IllegalStateException("not a string value");
    return stringValue;
  }

  public double doubleValue() {
    if (!isDouble)
      throw new IllegalStateException("not a double value");
    return doubleValue;
  }

  public boolean booleanValue() {
    if (!isBoolean)
      throw new IllegalStateException("not a boolean value");
    return booleanValue;
  }

  public long longValue() {
    if (!isLong)
      throw new IllegalStateException("not a long value");
    return longValue;
  }

  public boolean isBoolean() {
    return isBoolean;
  }

  public boolean isDouble() {
    return isDouble;
  }

  public boolean isString() {
    return isString;
  }

  public boolean isLong() {
    return isLong;
  }

  @Override
  public String toString() {
    if (isString)
      return '\"' + stringValue + '\"';
    if (isDouble)
      return String.valueOf(doubleValue);
    if (isBoolean)
      return String.valueOf(booleanValue);
    if (isLong)
      return String.valueOf(longValue);
    return null;
  }

  public static ValueArrayBuilder valueArrayBuilder() {
    return new ValueArrayBuilder();
  }

  public static class ValueArrayBuilder {

    private final List<DBFValue> values = new ArrayList<>();

    public ValueArrayBuilder add(DBFValue... value) {
      for (var v : value)
        values.add(v);
      return this;
    }

    public ValueArrayBuilder add(String... value) {
      for (var v : value)
        values.add(new DBFValue(v));
      return this;
    }

    public ValueArrayBuilder add(double... value) {
      for (var v : value)
        values.add(new DBFValue(v));
      return this;
    }

    public ValueArrayBuilder add(boolean... value) {
      for (var v : value)
        values.add(new DBFValue(v));
      return this;
    }

    public ValueArrayBuilder add(long... value) {
      for (var v : value)
        values.add(new DBFValue(v));
      return this;
    }

    public List<DBFValue> buildList() {
      return values;
    }

    public DBFValue[] buildArray() {
      return values.toArray(new DBFValue[0]);
    }
  }

  public static List<DBFValue> stringValues(String... values) {
    List<DBFValue> ret = new ArrayList<>();
    for (var value : values)
      ret.add(new DBFValue(value));
    return ret;
  }

  public static List<DBFValue> doubleValues(double... values) {
    List<DBFValue> ret = new ArrayList<>();
    for (var value : values)
      ret.add(new DBFValue(value));
    return ret;
  }

  public static List<DBFValue> longValues(long... values) {
    List<DBFValue> ret = new ArrayList<>();
    for (var value : values)
      ret.add(new DBFValue(value));
    return ret;
  }

  public static List<DBFValue> booleanValues(boolean... values) {
    List<DBFValue> ret = new ArrayList<>();
    for (var value : values)
      ret.add(new DBFValue(value));
    return ret;
  }
}
