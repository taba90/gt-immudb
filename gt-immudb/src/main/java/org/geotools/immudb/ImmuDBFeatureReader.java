package org.geotools.immudb;

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

    private SimpleFeature next;
    protected Transaction tx;
    public ImmuDBFeatureReader(ImmuDBDataStore dataStore, ContentState state, SimpleFeatureType simpleFeatureType, String sql,SQLValue[] params) throws IOException {
        this.simpleFeatureType=simpleFeatureType;
        this.statement=new ImmuDBStatement(sql,dataStore.open(Transaction.AUTO_COMMIT),params);
        this.immuDBDataStore=dataStore;
        this.state=state;
    }
    @Override
    public SimpleFeatureType getFeatureType() {
        return simpleFeatureType;
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        SimpleFeature result=next;
        next=null;
        return result;
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            if (queryResult==null) queryResult=statement.executeQuery();
            if  (next==null && queryResult.next()){
                int attributeCount=simpleFeatureType.getAttributeCount();
                SimpleFeatureBuilder builder=new SimpleFeatureBuilder(simpleFeatureType);
                Object id=Converter.getValue(queryResult,0,immuDBDataStore.extractPkType(simpleFeatureType));
                String fid=new StringBuilder().append(simpleFeatureType.getTypeName()).append(".").append(id).toString();
                for (int i=0; i<attributeCount; i++){
                    AttributeDescriptor descriptor = simpleFeatureType.getDescriptor(i);
                    Object value = Converter.getValue(queryResult, i+1, descriptor.getType().getBinding());
                    builder.add(value);
                }
                next= builder.buildFeature(fid);
            }
            return next !=null;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (queryResult!=null) queryResult.close();
            immuDBDataStore.releaseConnection(state.getTransaction(),statement.getImmuClient());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

}
