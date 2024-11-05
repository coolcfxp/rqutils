package com.ricequant.rqutils.io_tools.csv;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author kangol
 */
public class CSVRow {

  private final Map<String, String> values = new LinkedHashMap<>();

  public CSVRow put(String name, String value) {
    values.put(name, value);
    return this;
  }

  public String get(String... name) {
    if (name.length == 0)
      return null;

    if (name.length == 1)
      return values.get(name[0]);

    for (int i = 0; i < name.length; i++) {
      String ret = values.get(name[i]);
      if (ret != null)
        return ret;
    }

    return null;
  }

  public boolean containsKey(String... names) {
    for (String name : names) {
      if (values.containsKey(name))
        return true;
    }
    return false;
  }

  public Map<String, String> values() {
    return values;
  }

  @Override
  public String toString() {
    return values.toString();
  }
}
