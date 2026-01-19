package com.ricequant.rqutils.io_tools.csv;

import com.ricequant.rqutils.io_tools.TableRowSource;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author kangol
 */
public class CSVRow implements TableRowSource {

  private final Map<String, String> values = new LinkedHashMap<>();

  public CSVRow put(String name, String value) {
    values.put(name, value);
    return this;
  }

  @Override
  public String getString(String... name) {
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

  @Override
  public CSVValue get(String... name) {
    if (name.length == 0)
      return null;

    if (name.length == 1)
      return new CSVValue(values.get(name[0]), true);

    for (int i = 0; i < name.length; i++) {
      String ret = values.get(name[i]);
      if (ret != null)
        return new CSVValue(ret, true);
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

  // order is preserved by LinkedHashMap
  public Map<String, String> values() {
    return values;
  }

  @Override
  public String toString() {
    return values.toString();
  }

  public String[] valuesArray() {
    return values.values().toArray(new String[0]);
  }
}
