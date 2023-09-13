package org.geotools.immudb;

import io.codenotary.immudb4j.sql.SQLQueryResult;
import io.codenotary.immudb4j.sql.SQLValue;
import org.apache.commons.lang3.StringUtils;
import org.geotools.geometry.jts.WKBReader;
import org.geotools.util.Converters;
import org.geotools.util.DateTimeParser;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Converter {

    private static final DateFormat DATE_FORMAT=new SimpleDateFormat("yyyy-MM-dd'T'HHmmssSSS'Z'");

    private static final WKBReader WKT_READER=new WKBReader();

    private static final WKBWriter WKT_WRITER=new WKBWriter();

    public static Geometry toGeometry(byte[] wkt) throws ParseException {
        if  (wkt==null) return null;

        return WKT_READER.read(wkt);
    }

    public static byte[] toByteArray(Geometry geometry) throws ParseException {
        if  (geometry==null) return null;
        return WKT_WRITER.write(geometry);
    }

    public static byte[] toByteArray(Double value){
        if  (value==null) return null;
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    public static Double toDouble(byte[] dbytes){
        if  (dbytes==null) return null;
        return ByteBuffer.wrap(dbytes).getDouble();
    }

    public static byte[] toByteArray(Float value){
        if  (value==null) return null;
        byte[] bytes = new byte[4];
        ByteBuffer.wrap(bytes).putFloat(value);
        return bytes;
    }

    public static Float toFloat(byte[] dbytes){
        if  (dbytes==null) return null;
        return ByteBuffer.wrap(dbytes).getFloat();
    }

    public static byte[] toByteArray(Long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static Long toLong(byte[] lbytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(lbytes);
        buffer.flip();
        return buffer.getLong();
    }

    public static byte[] toByteArray(Integer value) {
        return  ByteBuffer.allocate(4).putInt(value).array();
    }


    public static Integer toInteger(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static Date toDate(byte[] toDate) throws java.text.ParseException {
        String value=new String(toDate);
        return DATE_FORMAT.parse(value);
    }

    public static byte[] toByteArray(Boolean bool) {
        String value=bool.toString();
        return value.getBytes();
    }

    public static Boolean toBoolean(byte[] tBoolean) {
        String value=new String(tBoolean);
        return Boolean.valueOf(value);
    }

    public static byte[] toByteArray(Date date) {
        String value= DATE_FORMAT.format(date);
        return value.getBytes();
    }

    public static String asString(byte[] sbytes) {
        return new String(sbytes);
    }

    public static byte[] toByteArray(String value) {
        return value.getBytes();
    }

    static SQLValue getSQLValue(Object value) throws IOException {
        return getSQLValue(value,null);
    }
    static SQLValue getSQLValue(Object value, Class<?> binding) throws IOException {
        SQLValue sqlValue=null;
        try {
            if (Long.class.isAssignableFrom(getBinding(value,binding))) {
                sqlValue = new SQLValue((Long) value);
            } else if (Integer.class.isAssignableFrom(getBinding(value,binding))) {
                sqlValue = new SQLValue((Integer) value);
            } else if (Boolean.class.isAssignableFrom(getBinding(value,binding))) {
                sqlValue = new SQLValue((Boolean) value);
            } else if (Date.class.isAssignableFrom(getBinding(value,binding))) {
                sqlValue = new SQLValue((Date) value);
            } else if (String.class.isAssignableFrom(getBinding(value,binding))) {
                sqlValue = new SQLValue((String) value);
            } else {
                if (Geometry.class.isAssignableFrom(getBinding(value,binding))) {
                    sqlValue = new SQLValue(Converter.toByteArray((Geometry) value));
                } else if (Double.class.isAssignableFrom(getBinding(value,binding))) {
                    sqlValue = new SQLValue(Converter.toByteArray((Double) value));
                } else if (Float.class.isAssignableFrom(getBinding(value,binding))) {
                    sqlValue = new SQLValue(Converter.toByteArray((Float) value));
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        } catch (Exception e){
            throw new IOException(e);
        }
        return sqlValue;
    }

    static Class<?> getBinding(Object value, Class<?> clazz){
        if (clazz!=null) return clazz;
        else if (value!=null) return clazz.getClass();
        else return Object.class;
    }


    static Object getValue(SQLQueryResult queryResult, int index, Class<?> type) throws IOException {
        Object result=null;
        try {
            if (Long.class.isAssignableFrom(type)) {
                result = queryResult.getLong(index);
            } else if (Integer.class.isAssignableFrom(type)) {
                result = queryResult.getInt(index);
            } else if (Boolean.class.isAssignableFrom(type)) {
                result = queryResult.getBoolean(index);
            } else if (Date.class.isAssignableFrom(type)) {
                result = queryResult.getDate(index);
            } else if (String.class.isAssignableFrom(type)) {
                result = queryResult.getString(index);
            } else {
                byte[] bytes=queryResult.getBytes(index);
                if (Geometry.class.isAssignableFrom(type)) {
                    result=Converter.toGeometry(bytes);
                } else if (Double.class.isAssignableFrom(type)) {
                    result=Converter.toDouble(bytes);
                } else if (Float.class.isAssignableFrom(type)) {
                    result=Converter.toFloat(bytes);
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        } catch (Exception e){
            throw new IOException(e);
        }
        return result;
    }

    public static String getSQLType(Class<?> clazz){
        String result;
        if (Double.class.isAssignableFrom(clazz)){
            result="BLOB[8]";
        } else if (Float.class.isAssignableFrom(clazz)){
            result="BLOB[4]";
        } else if (Integer.class.isAssignableFrom(clazz) || Long.class.isAssignableFrom(clazz)){
            result="INTEGER";
        } else if (Geometry.class.isAssignableFrom(clazz)){
            result="BLOB";
        } else if (String.class.isAssignableFrom(clazz)){
            result="VARCHAR";
        } else {
            throw new UnsupportedOperationException(String.format("Type not supported %s", clazz.getSimpleName()));
        }
        return result;
    }


    public static SQLValue [] toSQLValues(SimpleFeature simpleFeature,Class<?> pkType, List<AttributeDescriptor> descriptorList) throws IOException {
        List<SQLValue> values=new ArrayList<>(descriptorList.size()-1);
        String fid=simpleFeature.getID();
        if (StringUtils.isNotBlank(fid) && fid.startsWith(simpleFeature.getType().getTypeName())){
            fid=fid.split("\\.")[1];
            values.add(Converter.getSQLValue(Converters.convert(fid,pkType)));
        }
        for (int i=0; i<descriptorList.size(); i++){
            AttributeDescriptor ad=descriptorList.get(i);
            Object value = simpleFeature.getAttribute(ad.getName());
            values.add(Converter.getSQLValue(value,ad.getType().getBinding()));
        }
        return values.toArray(new SQLValue[]{});
    }


    public static Object toValue(byte[] bytes, Class<?> type){
        Object result=null;
        if  (bytes !=null) {
            if (Double.class.isAssignableFrom(type)) {
                result=toDouble(bytes);
            } else if (Long.class.isAssignableFrom(type)) {
                result=toLong(bytes);
            } else if (Integer.class.isAssignableFrom(type)) {
                result=toInteger(bytes);
            } else if (Float.class.isAssignableFrom(type)) {
                result=toFloat(bytes);
            } else if (Boolean.class.isAssignableFrom(type)) {
                result=toBoolean(bytes);
            } else if (String.class.isAssignableFrom(type)) {
                result=asString(bytes);
            } else if (Date.class.isAssignableFrom(type)) {
                try {
                    result=toDate(bytes);
                } catch (java.text.ParseException e) {
                    throw new RuntimeException(e);
                }
            } else if (Geometry.class.isAssignableFrom(type)) {
                try {
                    result=toGeometry(bytes);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return result;
    }

    public static byte[] toByteArray(Object o, Class<?> type){
        byte[] bytes=null;
        if  (o !=null) {
            if (Double.class.isAssignableFrom(type)) {
                bytes=toByteArray((Double) o);
            } else if (Long.class.isAssignableFrom(type)) {
                bytes=toByteArray((Long) o);
            } else if (Integer.class.isAssignableFrom(type)) {
                bytes=toByteArray((Integer) o);
            } else if (Float.class.isAssignableFrom(type)) {
                bytes=toByteArray((Float) o);
            } else if (Boolean.class.isAssignableFrom(type)) {
                bytes=toByteArray((Boolean) o);
            } else if (String.class.isAssignableFrom(type)) {
                bytes=toByteArray((String) o);
            } else if (Date.class.isAssignableFrom(type)) {
                bytes=toByteArray((Date) o);
            } else if (Geometry.class.isAssignableFrom(type)) {
                try {
                    bytes=toByteArray((Geometry) o);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return bytes;
    }
}
