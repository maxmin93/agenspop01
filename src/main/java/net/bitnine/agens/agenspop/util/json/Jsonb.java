package net.bitnine.agens.agenspop.util.json;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.Serializable;
import java.sql.SQLException;

public class Jsonb extends PGobject implements Serializable, Cloneable {
    private Object jsonValue = null;

    public Jsonb() {
        setType("jsonb");
    }

    Jsonb(Object obj) {
        this();

        jsonValue = obj;
    }

    @Override
    public void setValue(String value) throws SQLException {
        Object obj;
        try {
            obj = JSONValue.parseWithException(value);
        } catch (Exception e) {
            throw new PSQLException("Parsing jsonb failed", PSQLState.DATA_ERROR, e);
        }

        super.setValue(value);

        this.jsonValue = obj;
    }

    @Override
    public String getValue() {
        if (value == null)
            value = JSONValue.toJSONString(jsonValue);

        return value;
    }

    public Object getJsonValue() {
        return jsonValue;
    }

    private String getString(Object obj) {
        if (obj instanceof String)
            return (String) obj;

        throw new UnsupportedOperationException("Not a string: " + obj);
    }

    private int getInt(Object obj) {
        if (obj instanceof Long) {
            long l = (Long) obj;
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE)
                throw new IllegalArgumentException("Bad value for type int: " + l);

            return (int) l;
        }

        throw new UnsupportedOperationException("Not an int: " + obj);
    }

    private long getLong(Object obj) {
        if (obj instanceof Long)
            return (Long) obj;

        throw new UnsupportedOperationException("Not a long: " + obj);
    }

    private double getDouble(Object obj) {
        if (obj instanceof Double)
            return (Double) obj;

        throw new UnsupportedOperationException("Not a double: " + obj);
    }

    private boolean getBoolean(Object obj) {
        if (obj instanceof Boolean)
            return (Boolean) obj;

        if (obj instanceof String)
            return ((String) obj).length() > 0;
        else if (obj instanceof Long)
            return (Long) obj != 0L;
        else if (obj instanceof Double)
            return (Double) obj != 0.0;
        else if (obj instanceof JSONArray)
            return ((JSONArray) obj).size() > 0;
        else if (obj instanceof JSONObject)
            return ((JSONObject) obj).size() > 0;
        else
            return false;
    }

    private Jsonb getArray(Object obj) {
        if (obj instanceof JSONArray)
            return new Jsonb(obj);

        throw new UnsupportedOperationException("Not an array: " + obj);
    }

    private Jsonb getObject(Object obj) {
        if (obj instanceof JSONObject)
            return new Jsonb(obj);

        throw new UnsupportedOperationException("Not an object: " + obj);
    }

    public String tryGetString() {
        if (jsonValue instanceof String)
            return (String) jsonValue;

        return null;
    }

    public String getString() {
        return getString(jsonValue);
    }

    public int getInt() {
        return getInt(jsonValue);
    }

    public long getLong() {
        return getLong(jsonValue);
    }

    public double getDouble() {
        return getDouble(jsonValue);
    }

    public boolean getBoolean() {
        return getBoolean(jsonValue);
    }

    public boolean isNull() {
        return jsonValue == null;
    }

    public Jsonb getArray() {
        return getArray(jsonValue);
    }

    public Jsonb getObject() {
        return getObject(jsonValue);
    }

    public String getString(int index) {
        if (!(jsonValue instanceof JSONArray))
            throw new UnsupportedOperationException("Not an array: " + jsonValue);

        JSONArray a = (JSONArray) jsonValue;
        return getString(a.get(index));
    }

    public int getInt(int index) {
        if (!(jsonValue instanceof JSONArray))
            throw new UnsupportedOperationException("Not an array: " + jsonValue);

        JSONArray a = (JSONArray) jsonValue;
        return getInt(a.get(index));
    }

    public long getLong(int index) {
        if (!(jsonValue instanceof JSONArray))
            throw new UnsupportedOperationException("Not an array: " + jsonValue);

        JSONArray a = (JSONArray) jsonValue;
        return getLong(a.get(index));
    }

    public double getDouble(int index) {
        if (!(jsonValue instanceof JSONArray))
            throw new UnsupportedOperationException("Not an array: " + jsonValue);

        JSONArray a = (JSONArray) jsonValue;
        return getDouble(a.get(index));
    }

    public boolean getBoolean(int index) {
        if (!(jsonValue instanceof JSONArray))
            throw new UnsupportedOperationException("Not an array: " + jsonValue);

        JSONArray a = (JSONArray) jsonValue;
        return getBoolean(a.get(index));
    }

    public boolean isNull(int index) {
        if (!(jsonValue instanceof JSONArray))
            throw new UnsupportedOperationException("Not an array: " + jsonValue);

        JSONArray a = (JSONArray) jsonValue;
        return a.get(index) == null;
    }

    public Jsonb getArray(int index) {
        if (!(jsonValue instanceof JSONArray))
            throw new UnsupportedOperationException("Not an array: " + jsonValue);

        JSONArray a = (JSONArray) jsonValue;
        return getArray(a.get(index));
    }

    public Jsonb getObject(int index) {
        if (!(jsonValue instanceof JSONArray))
            throw new UnsupportedOperationException("Not an array: " + jsonValue);

        JSONArray a = (JSONArray) jsonValue;
        return getObject(a.get(index));
    }

    public String getString(String key) {
        if (!(jsonValue instanceof JSONObject))
            throw new UnsupportedOperationException("Not an object: " + jsonValue);

        JSONObject o = (JSONObject) jsonValue;
        return getString(o.get(key));
    }

    public int getInt(String key) {
        if (!(jsonValue instanceof JSONObject))
            throw new UnsupportedOperationException("Not an object: " + jsonValue);

        JSONObject o = (JSONObject) jsonValue;
        return getInt(o.get(key));
    }

    public long getLong(String key) {
        if (!(jsonValue instanceof JSONObject))
            throw new UnsupportedOperationException("Not an object: " + jsonValue);

        JSONObject o = (JSONObject) jsonValue;
        return getLong(o.get(key));
    }

    public double getDouble(String key) {
        if (!(jsonValue instanceof JSONObject))
            throw new UnsupportedOperationException("Not an object: " + jsonValue);

        JSONObject o = (JSONObject) jsonValue;
        return getDouble(o.get(key));
    }

    public boolean getBoolean(String key) {
        if (!(jsonValue instanceof JSONObject))
            throw new UnsupportedOperationException("Not an object: " + jsonValue);

        JSONObject o = (JSONObject) jsonValue;
        return getBoolean(o.get(key));
    }

    public boolean isNull(String key) {
        if (!(jsonValue instanceof JSONObject))
            throw new UnsupportedOperationException("Not an object: " + jsonValue);

        JSONObject o = (JSONObject) jsonValue;
        return o.get(key) == null;
    }

    public Jsonb getArray(String key) {
        if (!(jsonValue instanceof JSONObject))
            throw new UnsupportedOperationException("Not an object: " + jsonValue);

        JSONObject o = (JSONObject) jsonValue;
        return getArray(o.get(key));
    }

    public Jsonb getObject(String key) {
        if (!(jsonValue instanceof JSONObject))
            throw new UnsupportedOperationException("Not an object: " + jsonValue);

        JSONObject o = (JSONObject) jsonValue;
        return getObject(o.get(key));
    }

    public int size() {
        if (jsonValue instanceof JSONArray)
            return ((JSONArray) jsonValue).size();
        else if (jsonValue instanceof JSONObject)
            return ((JSONObject) jsonValue).size();
        else
            throw new UnsupportedOperationException("Not an array or an object: " + jsonValue);
    }

    @Override
    public String toString() {
        return getValue();
    }
}