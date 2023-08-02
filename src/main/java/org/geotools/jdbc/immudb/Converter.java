package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.sql.SQLQueryResult;
import io.codenotary.immudb4j.sql.SQLValue;
import org.geotools.geometry.jts.WKBReader;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.geometry.jts.WKTWriter2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Converter {

    private static final WKBReader WKT_READER=new WKBReader();

    private static final WKBWriter WKT_WRITER=new WKBWriter();

    public static Geometry toGeometry(byte[] wkt) throws ParseException {
        return WKT_READER.read(wkt);
    }

    public static byte[] toByteArray(Geometry geometry) throws ParseException {
        return WKT_WRITER.write(geometry);
    }

    public static byte[] toByteArray(Double value){
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    public static Double toDouble(byte[] dbytes){
        return ByteBuffer.wrap(dbytes).getDouble();
    }

    public static byte[] toByteArray(Float value){
        byte[] bytes = new byte[4];
        ByteBuffer.wrap(bytes).putFloat(value);
        return bytes;
    }

    public static Float toFloat(byte[] dbytes){
        return ByteBuffer.wrap(dbytes).getFloat();
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


    public static SQLValue [] toSQLValues(SimpleFeature simpleFeature, String pk, List<AttributeDescriptor> descriptorList) throws IOException {
        List<SQLValue> values=new ArrayList<>(descriptorList.size()-1);
        for (int i=0; i<descriptorList.size(); i++){
            AttributeDescriptor ad=descriptorList.get(i);
            if (!ad.getLocalName().equalsIgnoreCase(pk)) {
                Object value = simpleFeature.getAttribute(ad.getName());
                values.add(Converter.getSQLValue(value,ad.getType().getBinding()));
            }
        }
        return values.toArray(new SQLValue[]{});
    }
}
