package org.geotools.immudb;

import io.codenotary.immudb4j.sql.SQLValue;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.FilteringFeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;

public class ImmuDBFeatureStore extends ContentFeatureStore {

    private ImmuDBFeatureSource delegate;


    public ImmuDBFeatureStore(URI featureTypeUri, ImmuDBSessionParams sessionParams, ContentEntry entry, Query query) {

        super(entry, query);
        this.delegate=new ImmuDBFeatureSource(featureTypeUri,sessionParams,entry,query);
    }

    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int flags) throws IOException {
        FeatureWriter<SimpleFeatureType,SimpleFeature> writer=null;
        SimpleFeatureType schema= getSchema();
        try {
            if ((flags | WRITER_ADD) == WRITER_ADD) {
                Query queryNone = new Query(query);
                queryNone.setFilter(Filter.EXCLUDE);
                String sql = insertSQL();
                writer = new ImmuDBInsertFeatureWriter(this, getDataStore(), getState(), schema, sql);
            } else {
                Filter[] split = delegate.splitFilter(query.getFilter());
                Filter preFilter = split[0];
                Filter postFilter = split[1];
                boolean postFilterRequired = postFilter != null && postFilter != Filter.INCLUDE;
                Query preQuery = new Query(query);
                preQuery.setFilter(preFilter);
                ImmuDBDataStore dataStore = getDataStore();
                ImmuDBFilterToSQL filterToSQL = dataStore.getFilterToSQL(new StringWriter(), schema);
                String sql = dataStore.selectSQL(schema, filterToSQL, preQuery);
                SQLValue[] params = delegate.getParams(filterToSQL);
                writer = new ImmuDBUpsertFeatureWriter(this, getDataStore(), getState(), schema, sql, params);
                if (postFilterRequired) {
                    writer = new FilteringFeatureWriter(writer, postFilter);
                }
            }
        } catch (Exception e){
            if (writer!=null) writer.close();
            throw new IOException(e);
        }
        return writer;
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

    private String insertSQL() throws IOException {
        SimpleFeatureType simpleFeatureType=getSchema();
        String pkName= getDataStore().extractPkAttribute(simpleFeatureType);
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

    protected String updateSql(SimpleFeatureType simpleFeatureType, boolean includeId) throws IOException {
        ImmuDBDataStore dataStore=getDataStore();
        List<AttributeDescriptor> descriptorList=simpleFeatureType.getAttributeDescriptors();
        StringBuilder sql=new StringBuilder("UPSERT INTO ");
        sql.append(simpleFeatureType.getTypeName()).append(" (");
        if (includeId) sql.append(dataStore.extractPkAttribute(simpleFeatureType)).append(",");
        for (int i=0; i<descriptorList.size(); i++){
            AttributeDescriptor ad=descriptorList.get(i);
            sql.append(ad.getLocalName()).append(",");
        }
        sql.setLength(sql.length() - 1);
        sql.append(") ");
        sql.append(" VALUES (");
        if  (includeId) sql.append("?,");
        for (int i=0; i<descriptorList.size(); i++){
            AttributeDescriptor ad=descriptorList.get(i);
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

    @Override
    protected boolean canFilter() {
        return delegate.canFilter();
    }
}
