package com.ricequant.rqutils.io_tools;

/**
 * @author kangol
 */
public interface TableRowSource {

  String getString(String... name);

  TableValue get(String... name);
}
