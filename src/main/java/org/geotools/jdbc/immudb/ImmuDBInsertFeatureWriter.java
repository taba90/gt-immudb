package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.sql.SQLValue;
import org.geotools.data.FeatureWriter;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ImmuDBInsertFeatureWriter extends ImmuDBFeatureReader implements FeatureWriter<SimpleFeatureType, SimpleFeature> {

    private final SimpleFeature[] buffer;

    private int curBufferPos = 0;

    ContentFeatureSource featureSource;

    public ImmuDBInsertFeatureWriter(ContentFeatureSource featureSource,ImmuDBDataStore dataStore, ContentState state, SimpleFeatureType simpleFeatureType,String sql) throws IOException {
        super(dataStore,state, simpleFeatureType, sql,null);
        buffer = new SimpleFeature[500];
       this.featureSource=featureSource;
       this.immuDBDataStore=dataStore;
    }

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public SimpleFeature next() throws IOException {
        SimpleFeatureBuilder builder=new SimpleFeatureBuilder(getFeatureType());
        SimpleFeature f= builder.buildFeature(null);
        buffer[curBufferPos]=f;
        curBufferPos++;
        return f;
    }

    @Override
    public void remove() throws IOException {
        // noop
    }

    @Override
    public void write() throws IOException {
        if (++curBufferPos >= buffer.length) {
            // buffer full => do the inserts
            flush();
        }
    }

    private void flush() throws IOException {
        if (curBufferPos == 0) {
            return;
        }
        try {
            // do the insert
            Collection<SimpleFeature> features =
                    Arrays.asList(Arrays.copyOfRange(buffer, 0, curBufferPos));
            Class<?> pkType=immuDBDataStore.extractPkType(simpleFeatureType);
            List<AttributeDescriptor> descriptors=simpleFeatureType.getAttributeDescriptors();
            for (SimpleFeature cur : features) {
                if (cur!=null) {
                    // the datastore sets as userData, grab it and update the fid
                    SQLValue[] params = Converter.toSQLValues(cur, pkType, descriptors);
                    statement.executeStmt(params);
                    final ContentEntry entry = featureSource.getEntry();
                    final ContentState state = entry.getState(this.tx);
                    state.fireFeatureAdded(featureSource, cur);
                }
            }
        } finally {
            curBufferPos = 0;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            super.close();
        }
    }
}
