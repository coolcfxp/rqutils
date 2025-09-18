package com.ricequant.rqutils.io_tools.csv;

import com.ricequant.rqutils.io_tools.TableValue;

/**
 * @author kangol
 */
public class CSVValue implements TableValue {

  private String stringValue;

  private double doubleValue;

  private long longValue;

  private boolean booleanValue;

  private boolean isBoolean = false;

  private boolean isLong = false;

  private boolean isString = false;

  private boolean isDouble = false;

  private boolean isEmpty = false;

  public CSVValue(String genericValue, boolean tryParse) {
    if (genericValue == null || genericValue.trim().isEmpty()) {
      this.stringValue = genericValue;
      this.doubleValue = 0;
      this.longValue = 0;
      this.booleanValue = false;
      isEmpty = true;
      isString = true;
      return;
    }

    if (tryParse) {
      String trimmed = genericValue.trim();

      if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
        this.booleanValue = Boolean.parseBoolean(trimmed);
        isBoolean = true;
      }

      try {
        this.longValue = Long.parseLong(trimmed);
        isLong = true;
      }
      catch (NumberFormatException ignored) {
      }

      try {
        this.doubleValue = Double.parseDouble(trimmed);
        isDouble = true;
      }
      catch (NumberFormatException ignored) {
      }
    }

    this.stringValue = genericValue;
    isString = true;
  }

  public CSVValue(String stringValue) {
    this.stringValue = stringValue;
    this.doubleValue = 0;
    this.longValue = 0;
    this.booleanValue = false;
    isString = true;
    if (stringValue == null || stringValue.trim().isEmpty()) {
      isEmpty = true;
    }
  }

  public CSVValue(double doubleValue) {
    this.stringValue = null;
    this.doubleValue = doubleValue;
    this.longValue = 0;
    this.booleanValue = false;
    isDouble = true;
  }

  public CSVValue(long longValue) {
    this.stringValue = null;
    this.doubleValue = 0;
    this.longValue = longValue;
    this.booleanValue = false;
    isLong = true;
  }

  public CSVValue(boolean booleanValue) {
    this.stringValue = null;
    this.doubleValue = 0;
    this.longValue = 0;
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

  public long longValue() {
    if (!isLong)
      throw new IllegalStateException("not a long value");
    return longValue;
  }

  @Override
  public boolean booleanValue() {
    if (!isBoolean)
      throw new IllegalStateException("not a boolean value");
    return booleanValue;
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
  public boolean isBoolean() {
    return isBoolean;
  }

  @Override
  public CSVValue convertToString() {
    if (isLong) {
      stringValue = String.valueOf(longValue);
      longValue = 0;
      isLong = false;
    }
    else if (isDouble) {
      stringValue = String.valueOf(doubleValue);
      doubleValue = 0;
      isDouble = false;
    }
    else if (isBoolean) {
      stringValue = String.valueOf(booleanValue);
      booleanValue = false;
      isBoolean = false;
    }
    isString = true;
    return this;
  }

  @Override
  public CSVValue convertToDouble() {
    if (isLong) {
      doubleValue = longValue;
      longValue = 0;
      isLong = false;
    }
    else if (isString) {
      doubleValue = Double.parseDouble(stringValue);
      stringValue = null;
      isString = false;
    }
    else if (isBoolean) {
      doubleValue = booleanValue ? 1 : 0;
      booleanValue = false;
      isBoolean = false;
    }
    isDouble = true;
    return this;
  }

  @Override
  public CSVValue convertToLong() {
    if (isDouble) {
      longValue = (long) doubleValue;
      doubleValue = 0;
      isDouble = false;
    }
    else if (isString) {
      longValue = Long.parseLong(stringValue);
      stringValue = null;
      isString = false;
    }
    else if (isBoolean) {
      longValue = booleanValue ? 1 : 0;
      booleanValue = false;
      isBoolean = false;
    }
    isLong = true;
    return this;
  }

  @Override
  public CSVValue convertToBoolean() {
    if (isLong) {
      booleanValue = longValue != 0;
      longValue = 0;
      isLong = false;
    }
    else if (isString) {
      booleanValue = stringValue != null && (stringValue.equals("true"));
      stringValue = null;
      isString = false;
    }
    else if (isDouble) {
      booleanValue = doubleValue != 0;
      doubleValue = 0;
      isDouble = false;
    }
    isBoolean = true;
    return this;
  }

  public boolean isEmpty() {
    return isEmpty;
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
