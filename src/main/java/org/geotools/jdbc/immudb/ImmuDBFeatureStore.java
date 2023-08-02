package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.ImmuClient;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.List;

public class ImmuDBFeatureStore extends ContentFeatureStore {

    private ImmuDBFeatureSource delegate;


    public ImmuDBFeatureStore(URI featureTypeUri, ImmuDBSessionParams sessionParams, ImmuClient client, ContentEntry entry, Query query) {

        super(entry, query);
        this.delegate=new ImmuDBFeatureSource(featureTypeUri,sessionParams,client,entry,query);
    }

    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int flags) throws IOException {
        SimpleFeatureType simpleFeatureType=getSchema();
        String sql=insertSQL(simpleFeatureType);
        ImmuDBStatement statement=new ImmuDBStatement(sql,delegate.getImmuClient());
        ImmuDBInsertFeatureWriter featureWriter=new ImmuDBInsertFeatureWriter(this,getDataStore(),getState(),simpleFeatureType,statement);
        return featureWriter;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        return delegate.getBoundsInternal(query);
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        return delegate.getCountInternal(query);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        return delegate.getReaderInternal(query);
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return delegate.buildFeatureType();
    }

    private String insertSQL(SimpleFeatureType simpleFeatureType) throws IOException {
        String pkName= getDataStore().getPrimaryKey(simpleFeatureType);
        List<AttributeDescriptor> descriptorList=simpleFeatureType.getAttributeDescriptors();
        StringBuilder sql=new StringBuilder("INSERT INTO ");
        sql.append(simpleFeatureType.getTypeName()).append(" (");
        for (int i=0; i<descriptorList.size(); i++){
            AttributeDescriptor ad=descriptorList.get(i);
            if (ad.getLocalName().equals(pkName))
                continue;
            sql.append(ad.getLocalName()).append(",");
        }
        sql.setLength(sql.length() - 1);
        sql.append(") ");
        sql.append(" VALUES (");
        for (int i=0; i<descriptorList.size(); i++){
            AttributeDescriptor ad=descriptorList.get(i);
            if (ad.getLocalName().equals(pkName))
                continue;
            sql.append("?,");
        }
        sql.setLength(sql.length() - 1);
        sql.append(") ");
        return sql.toString();
    }

    @Override
    public ImmuDBDataStore getDataStore() {
        return (ImmuDBDataStore) entry.getDataStore();
    }
}
