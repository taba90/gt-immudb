package org.geotools.immudb;

import io.codenotary.immudb4j.sql.SQLValue;
import org.geotools.data.FeatureWriter;
import org.geotools.data.store.ContentState;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;

public class ImmuDBUpsertFeatureWriter extends ImmuDBFeatureReader implements FeatureWriter<SimpleFeatureType, SimpleFeature> {
    private SimpleFeature last;

    private ImmuDBFeatureStore featureStore;

    public ImmuDBUpsertFeatureWriter(ImmuDBFeatureStore featureStore, ImmuDBDataStore dataStore, ContentState state, SimpleFeatureType simpleFeatureType,String sql,SQLValue[] params) throws IOException {
        super(dataStore, state, simpleFeatureType, sql,params);
        this.featureStore =featureStore;
    }

    @Override
    public SimpleFeature next() throws IOException {
        last= super.next();
        return last;
    }



    @Override
    public void remove() throws IOException {

    }

    @Override
    public void write() throws IOException {
        String id=last.getID();
        boolean update=id!=null && id.startsWith(simpleFeatureType.getTypeName());
        SQLValue[] values=Converter.toSQLValues(last,immuDBDataStore.extractPkType(simpleFeatureType),simpleFeatureType.getAttributeDescriptors(),immuDBDataStore.isEncryptFeatureType(simpleFeatureType));
        String sql=featureStore.updateSql(simpleFeatureType,update);
        statement.executeStmt(values);
    }
}
