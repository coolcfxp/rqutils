package com.ricequant.rqutils.io_tools.csv;

/**
 * @author kangol
 */
public class CSVValue {

  private final String stringValue;

  private final double doubleValue;

  private final long longValue;

  private boolean isLong = false;

  private boolean isString = false;

  private boolean isDouble = false;

  public CSVValue(String stringValue) {
    this.stringValue = stringValue;
    this.doubleValue = 0;
    this.longValue = 0;
    isString = true;
  }

  public CSVValue(double doubleValue) {
    this.stringValue = null;
    this.doubleValue = doubleValue;
    this.longValue = 0;
    isDouble = true;
  }

  public CSVValue(long longValue) {
    this.stringValue = null;
    this.doubleValue = 0;
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


  public long longValue() {
    if (!isLong)
      throw new IllegalStateException("not a long value");
    return longValue;
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
    if (isLong)
      return String.valueOf(longValue);
    return null;
  }


}
