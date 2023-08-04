package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.sql.SQLException;
import io.codenotary.immudb4j.sql.SQLValue;
import org.geotools.data.FeatureReader;
import org.geotools.data.FilteringFeatureReader;
import org.geotools.data.MaxFeatureReader;
import org.geotools.data.Query;
import org.geotools.data.ReTypeFeatureReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.filter.visitor.PostPreProcessFilterSplittingVisitor;
import org.geotools.filter.visitor.SimplifyingFilterVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.geotools.jdbc.immudb.Converter.getSQLValue;

public class ImmuDBFeatureSource extends ContentFeatureSource {


    private URI featureTypeUri;
    private ImmuDBSessionParams sessionParams;

    /**
     * Creates the new feature source from a query.
     *
     * <p>The <tt>query</tt> is taken into account for any operations done against the feature
     * source. For example, when getReader(Query) is called the query specified is "joined" to the
     * query specified in the constructor. The <tt>query</tt> parameter may be <code>null</code> to
     * specify that the feature source represents the entire set of features.
     *
     * @param entry
     * @param query
     */
    public ImmuDBFeatureSource(URI featureTypeUri, ImmuDBSessionParams sessionParams, ContentEntry entry, Query query) {
        super(entry, query);
        this.featureTypeUri=featureTypeUri;
        this.sessionParams=sessionParams;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        FeatureReader<SimpleFeatureType,SimpleFeature> reader =getReader(query);
        ReferencedEnvelope bounds=new ReferencedEnvelope(getSchema().getCoordinateReferenceSystem());
        while(reader.hasNext()){
            SimpleFeature sf=reader.next();
            bounds.expandToInclude(((Geometry)sf.getDefaultGeometry()).getEnvelopeInternal());
        }
        return bounds;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        return 0;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        Filter[] split = splitFilter(query.getFilter());
        Filter preFilter = split[0];
        Filter postFilter = split[1];
        boolean postFilterRequired = postFilter != null && postFilter != Filter.INCLUDE;


        ImmuDBDataStore immuDBDataStore=(ImmuDBDataStore) getDataStore();
        // rebuild a new query with the same params, but just the pre-filter
        Query preQuery = new Query(query);
        preQuery.setFilter(preFilter);
        // in case of post filtering, we cannot do native paging
        if (postFilterRequired) {
            preQuery.setStartIndex(0);
            preQuery.setMaxFeatures(Integer.MAX_VALUE);
        }
        // Build the feature type returned by this query. Also build an eventual extra feature type
        // containing the attributes we might need in order to evaluate the post filter
        SimpleFeatureType[] types =
                buildQueryAndReturnFeatureTypes(getSchema(), query.getPropertyNames(), postFilter);
        SimpleFeatureType querySchema = types[0];
        SimpleFeatureType returnedSchema = types[1];
        ImmuDBFilterToSQL immuDBFilterToSQL=((ImmuDBDataStore)getDataStore()).getFilterToSQL(new StringWriter(),getSchema());
        FeatureReader<SimpleFeatureType, SimpleFeature> reader=null;
        try {
            String sql = immuDBDataStore.selectSQL(querySchema, immuDBFilterToSQL, preQuery);
            SQLValue[] params = getParams(immuDBFilterToSQL);
            reader=new ImmuDBFeatureReader((ImmuDBDataStore) getDataStore(),getState(),querySchema,sql,params);
            // if post filter, wrap it
            if (postFilterRequired) {
                reader = new FilteringFeatureReader<>(reader, postFilter);
                if (!returnedSchema.equals(querySchema)) {
                    reader = new ReTypeFeatureReader(reader, returnedSchema);
                }

                // offset
                int offset = query.getStartIndex() != null ? query.getStartIndex() : 0;
                if (offset > 0) {
                    // skip the first n records
                    for (int i = 0; i < offset && reader.hasNext(); i++) {
                        reader.next();
                    }
                }

                // max feature limit
                if (query.getMaxFeatures() >= 0 && query.getMaxFeatures() < Integer.MAX_VALUE) {
                    reader = new MaxFeatureReader<>(reader, query.getMaxFeatures());
                }
            }
            return reader;
        } catch (Exception e){
            if (reader!=null) reader.close();
            throw new IOException(e);
        }

    }

    protected Filter[] splitFilter(Filter original) {
        return splitFilter(original, this);
    }

    Filter[] splitFilter(Filter original, SimpleFeatureSource source) {

        Filter[] split = new Filter[2];
        if (original != null) {
            PostPreProcessFilterSplittingVisitor splitter =
                    new ImmuDBPostPreProcessFilterSplittingVisitor(ImmuDBFilterToSQL.createCapabilities(), source.getSchema(), null);
            original.accept(splitter, null);
            split = new Filter[2];
            split[0] = splitter.getFilterPre();
            split[1] = splitter.getFilterPost();

            // handle three-valued logic differences by adding "is not null" checks in the filter,
            // the simplifying filter visitor will take care of them if they are redundant

            SimplifyingFilterVisitor visitor = new SimplifyingFilterVisitor();
            visitor.setFeatureType(getSchema());
            split[0] = (Filter) split[0].accept(visitor, null);
            split[1] = (Filter) split[1].accept(visitor, null);
        }

        return split;
    }

    SimpleFeatureType[] buildQueryAndReturnFeatureTypes(
            SimpleFeatureType featureType, String[] propertyNames, Filter filter) {

        SimpleFeatureType[] types = null;
        if (propertyNames == Query.ALL_NAMES) {
            return new SimpleFeatureType[] {featureType, featureType};
        } else {
            SimpleFeatureType returnedSchema =
                    SimpleFeatureTypeBuilder.retype(featureType, propertyNames);
            SimpleFeatureType querySchema = returnedSchema;

            if (filter != null && !filter.equals(Filter.INCLUDE)) {
                FilterAttributeExtractor extractor = new FilterAttributeExtractor(featureType);
                filter.accept(extractor, null);

                String[] extraAttributes = extractor.getAttributeNames();
                if (extraAttributes != null && extraAttributes.length > 0) {
                    List<String> allAttributes = new ArrayList<>(Arrays.asList(propertyNames));
                    for (String extraAttribute : extraAttributes) {
                        if (!allAttributes.contains(extraAttribute))
                            allAttributes.add(extraAttribute);
                    }
                    String[] allAttributeArray =
                            allAttributes.toArray(new String[allAttributes.size()]);
                    querySchema = SimpleFeatureTypeBuilder.retype(getSchema(), allAttributeArray);
                }
            }
            types = new SimpleFeatureType[] {querySchema, returnedSchema};
        }
        return types;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return new GeoJSONToFeatureType(featureTypeUri,entry.getName().getNamespaceURI()).readType();
    }

    protected SQLValue[] getParams(ImmuDBFilterToSQL toSQL) throws IOException {
        SQLValue[] params = new SQLValue[toSQL.getLiteralValues().size()];
        List<Object> literals = toSQL.getLiteralValues();
        for (int i = 0; i < literals.size(); i++) {
            params[i] = getSQLValue(literals.get(i));
        }
        return params;
    }
}
