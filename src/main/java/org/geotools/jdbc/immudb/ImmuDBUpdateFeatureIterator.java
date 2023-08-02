package org.geotools.jdbc.immudb;

import org.geotools.data.FeatureWriter;
import org.geotools.data.store.ContentState;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;

public class ImmuDBUpdateFeatureIterator extends ImmuDBFeatureReader implements FeatureWriter<SimpleFeatureType, SimpleFeature> {
    public ImmuDBUpdateFeatureIterator(ImmuDBDataStore dataStore, ContentState state, SimpleFeatureType simpleFeatureType, ImmuDBStatement statement) {
        super(dataStore, state, simpleFeatureType, statement);
    }

    @Override
    public void remove() throws IOException {

    }

    @Override
    public void write() throws IOException {

    }
}
