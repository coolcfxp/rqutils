package com.ricequant.rqutils.io_tools.dbf;

/**
 * @author kangol
 */
public class DBFValue {

  private final String stringValue;

  private final double doubleValue;

  private final boolean booleanValue;

  private boolean isString = false;

  private boolean isDouble = false;

  private boolean isBoolean = false;

  public DBFValue(String stringValue) {
    this.stringValue = stringValue;
    this.doubleValue = 0;
    this.booleanValue = false;
    isString = true;
  }

  public DBFValue(double doubleValue) {
    this.stringValue = null;
    this.doubleValue = doubleValue;
    this.booleanValue = false;
    isDouble = true;
  }

  public DBFValue(boolean booleanValue) {
    this.stringValue = null;
    this.doubleValue = 0;
    this.booleanValue = booleanValue;
    isBoolean = true;
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

  public boolean isBoolean() {
    return isBoolean;
  }

  public boolean isDouble() {
    return isDouble;
  }

  public boolean isString() {
    return isString;
  }

  @Override
  public String toString() {
    if (isString)
      return '\"' + stringValue + '\"';
    if (isDouble)
      return String.valueOf(doubleValue);
    if (isBoolean)
      return String.valueOf(booleanValue);
    return null;
  }
}
