package com.ricequant.rqutils.io_tools.csv;

/**
 * @author kangol
 */
public class CSVField {
    private final String name;

    private final byte type;

    public static final byte FIELD_TYPE_STRING = 'C';
    public static final byte FIELD_TYPE_LONG = 'I';
    public static final byte FIELD_TYPE_DOUBLE = 'N';
    private int precision;

    public CSVField(String name, byte type) {
        this.name = name;
        this.type = type;
    }
    public CSVField(String name, byte type, int precision) {
        this.name = name;
        this.type = type;
        this.precision = precision;
    }

    @Override
    public String toString() {
        return String.format("name: %s, type: %s", name, type);
    }

    public CSVValue decode(String value) {
        if (type == 'C') {
            // String type
            return new CSVValue(value);
        }
        else if (type == 'N' || type == 'F') {
            // Number type
            return new CSVValue(Double.parseDouble(value));
        }
        else if (type == 'D') {
            // Date type
            return new CSVValue(Integer.parseInt(value));
        }

        return new CSVValue(value);
    }

    public String encode(CSVValue value) {
        if (value.isEmpty())
            return "";

        if (type == FIELD_TYPE_STRING) {
            return value.stringValue();
        }
        else if (type == FIELD_TYPE_LONG) {
            return String.format("%d", value.longValue());
        }
        else if (type == FIELD_TYPE_DOUBLE) {
            String format = String.format("%%.%df", precision);
            return String.format(format, value.doubleValue());
        }
        return null;
    }

    public String name() {
        return name;
    }

    public byte type() {
        return type;
    }

}
