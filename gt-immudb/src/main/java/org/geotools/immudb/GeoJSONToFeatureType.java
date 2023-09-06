package org.geotools.immudb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GeoJSONToFeatureType {

    private URI featureTypeUri;

    private static final String NAME="name";

    private static final String TYPES="types";

    private static final String TYPE="type";

    private static final String ATTRIBUTE="attribute";

    private static final String PRIMARY_KEY="primaryKey";

    public static final String PK_USER_DATA="PK";
    private static final List<Class<?>> SUPPORTED_TYPES= Arrays.asList(Float.class,Double.class,String.class,Integer.class,Long.class,byte[].class, Boolean.class, Date.class, Point.class,
    LineString.class, Polygon.class, MultiPoint.class, MultiLineString.class, MultiPolygon.class, Geometry.class);
    private String ns;
    public GeoJSONToFeatureType(URI featureTypeUri, String ns){
        this.ns=ns;
        this.featureTypeUri=featureTypeUri;
    }

    public SimpleFeatureType readType() throws IOException {
        ObjectMapper objectMapper=new ObjectMapper();
        SimpleFeatureTypeBuilder typeBuilder=new SimpleFeatureTypeBuilder();
        typeBuilder.setSRS("EPSG:4326");
        JsonNode jn=objectMapper.readTree(featureTypeUri.toURL());
        if (!jn.has(NAME)) throw new RuntimeException("The JSON type must have a name attribute with the feature type name");
        if (!jn.has(TYPES)) throw new RuntimeException("The JSON type doesn't have any types definition");
        typeBuilder.setName(new NameImpl(ns,jn.get(NAME).asText()));
        JsonNode arrays=jn.get(TYPES);
        if (!arrays.isArray()) throw new RuntimeException("Types attribute must be an array");
        ArrayNode types=(ArrayNode) arrays;
        Map<String,Object> userData=new HashMap<>();
        for (int i=0; i<types.size(); i++){
            ObjectNode node=(ObjectNode) types.get(i);
            readAttributeType(node,typeBuilder,userData);
        }
        SimpleFeatureType sft= typeBuilder.buildFeatureType();
        sft.getUserData().putAll(userData);

        return sft;
    }

    private void readAttributeType(ObjectNode node, SimpleFeatureTypeBuilder simpleFeatureTypeBuilder, Map<String,Object> userData) throws IOException {
            String attribute = node.get(ATTRIBUTE).asText();
            String type = node.get(TYPE).asText();
            Boolean pk = false;
            if (node.has(PRIMARY_KEY)) {
                pk = node.get(PRIMARY_KEY).asBoolean();
            }
            if (pk.booleanValue()) {
                Class<?> clazz=getClassFromBinding(type);
                userData.put(PK_USER_DATA, new ImmuDBPk(simpleFeatureTypeBuilder.getName(),attribute,clazz));
            }
            if (!pk.booleanValue()){
                Class<?> clazz= getClassFromBinding(type);
                if (Geometry.class.isAssignableFrom(clazz))
                    simpleFeatureTypeBuilder.setDefaultGeometry(attribute);
                simpleFeatureTypeBuilder.add(attribute,clazz);
            }
    }

    private Class<?> getClassFromBinding(String type){
        Iterator<Class<?>> iterator=SUPPORTED_TYPES.iterator();
        Class<?> result=null;
        while(result==null && iterator.hasNext()){
            result=getIfEquals(iterator.next(),type);
        }
        return result;
    }

    private Class<?> getIfEquals(Class<?> clazz, String type){
        if (type.toUpperCase().equals(clazz.getSimpleName().toUpperCase()))
            return clazz;
        return null;
    }
}
