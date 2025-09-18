package com.ricequant.rqutils.io_tools;

/**
 * @author kangol
 */
public interface TableValue {

  String stringValue();

  double doubleValue();

  long longValue();

  boolean booleanValue();

  boolean isString();

  boolean isDouble();

  boolean isLong();

  boolean isBoolean();

  TableValue convertToString();

  TableValue convertToDouble();

  TableValue convertToLong();

  TableValue convertToBoolean();

}
