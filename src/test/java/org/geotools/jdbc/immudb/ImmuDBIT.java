package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.sql.SQLException;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.FilterFactory2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.geotools.jdbc.immudb.ImmuDBDataStoreFactory.DATABASE;
import static org.geotools.jdbc.immudb.ImmuDBDataStoreFactory.HOST;
import static org.geotools.jdbc.immudb.ImmuDBDataStoreFactory.JSON_SCHEMA;
import static org.geotools.jdbc.immudb.ImmuDBDataStoreFactory.NAMESPACE;
import static org.geotools.jdbc.immudb.ImmuDBDataStoreFactory.PASSWD;
import static org.geotools.jdbc.immudb.ImmuDBDataStoreFactory.PORT;
import static org.geotools.jdbc.immudb.ImmuDBDataStoreFactory.STATE_HOLDER_PATH;
import static org.geotools.jdbc.immudb.ImmuDBDataStoreFactory.USER;

public class ImmuDBIT {


    ImmuDBDataStore immuDBDataStore;

    FilterFactory2 FF = CommonFactoryFinder.getFilterFactory2();

    @Before
    public void setUPDataStore() throws IOException, URISyntaxException {
        String schemaPath=getClass().getResource("ft1-schema.json").toExternalForm();
        Map<String,Object> params=new HashMap<>();
        params.put(DATABASE.key,"defaultdb");
        params.put(HOST.key,"127.0.0.1");
        params.put(PORT.key,3322);
        params.put(USER.key,"immudb");
        params.put(PASSWD.key, "immudb");
        params.put(JSON_SCHEMA.key, schemaPath);
        params.put(STATE_HOLDER_PATH.key,"/home/mvolpini/workspace/state");
        params.put(NAMESPACE.key, "gt");
        GeoJSONToFeatureType geoJSONToFeatureType=new GeoJSONToFeatureType(new URI(schemaPath),"gt");
        SimpleFeatureType simpleFeatureType=geoJSONToFeatureType.readType();
        immuDBDataStore=new ImmuDBDataStoreFactory().createDataStore(params);
        immuDBDataStore.createSchema(simpleFeatureType);
        SimpleFeatureBuilder builder=new SimpleFeatureBuilder(simpleFeatureType);
        AttributeDescriptor ad=simpleFeatureType.getDescriptor("geom");
        builder.set(ad.getName(),new GeometryFactory().createPoint(new Coordinate(0,0)));
        ad=simpleFeatureType.getDescriptor("doubleproperty");
        builder.set(ad.getName(),0.1);
        ad=simpleFeatureType.getDescriptor("stringproperty");
        builder.set(ad.getName(),"zero");
        ad=simpleFeatureType.getDescriptor("intproperty");
        builder.set(ad.getName(),0);
        SimpleFeatureCollection collection=DataUtilities.collection(builder.buildFeature(null));
        SimpleFeatureStore store=(SimpleFeatureStore) immuDBDataStore.getFeatureSource(new NameImpl("gt","ft2"));
        store.addFeatures(collection);
    }

    @Test
    public void testGetFeatures() throws IOException {
        SimpleFeatureSource contentFeatureSource=immuDBDataStore.getFeatureSource(new NameImpl("gt","ft2"));
        SimpleFeatureCollection sfc=contentFeatureSource.getFeatures();
        SimpleFeatureIterator sfi=sfc.features();
        while(sfi.hasNext()){
            SimpleFeature sf=sfi.next();
            sf.getDefaultGeometry();
        }

    }

    @After
    public void tearDown() {
        if (immuDBDataStore!=null) {
            immuDBDataStore.dispose();
            this.immuDBDataStore = null;
        }
    }
}
