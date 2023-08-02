package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.ImmuClient;
import io.codenotary.immudb4j.sql.SQLException;
import io.codenotary.immudb4j.sql.SQLQueryResult;
import io.codenotary.immudb4j.sql.SQLValue;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import java.io.IOException;
import java.util.NoSuchElementException;

public class ImmuDBFeatureReader implements SimpleFeatureReader {

    protected SimpleFeatureType simpleFeatureType;

    private SQLQueryResult queryResult;
    protected ImmuDBDataStore immuDBDataStore;

    protected ContentState state;

    protected ImmuDBStatement statement;

    protected Transaction tx;
    public ImmuDBFeatureReader(ImmuDBDataStore dataStore, ContentState state, SimpleFeatureType simpleFeatureType, ImmuDBStatement statement){
        this.simpleFeatureType=simpleFeatureType;
        this.statement=statement;
        this.immuDBDataStore=dataStore;
        this.state=state;
    }
    @Override
    public SimpleFeatureType getFeatureType() {
        return simpleFeatureType;
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        int attributeCount=simpleFeatureType.getAttributeCount();
        SimpleFeatureBuilder builder=new SimpleFeatureBuilder(simpleFeatureType);
        for (int i=0; i<attributeCount; i++){
            AttributeDescriptor descriptor=simpleFeatureType.getDescriptor(i);
            Object value=Converter.getValue(queryResult,i,descriptor.getType().getBinding());
            builder.add(value);
        }
        return builder.buildFeature(null);

    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            if (queryResult==null) queryResult=statement.executeQuery();
            return queryResult.next();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (queryResult!=null) queryResult.close();
            immuDBDataStore.releaseConnection(state.getTransaction());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

}
