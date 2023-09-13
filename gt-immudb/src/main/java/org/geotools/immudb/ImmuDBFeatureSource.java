package org.geotools.immudb;

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
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.geotools.immudb.Converter.getSQLValue;
import static org.geotools.immudb.GeoJSONToFeatureType.ENCRYPT;

public class ImmuDBFeatureSource extends ContentFeatureSource {


    private SimpleFeatureType simpleFeatureType;
    public static final Hints.OptionKey SECRET_KEY=new Hints.OptionKey("secretKey");

    public static final Hints.OptionKey IV=new Hints.OptionKey("IV");
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
    public ImmuDBFeatureSource(URI featureTypeUri, ContentEntry entry, Query query) throws IOException {
        super(entry, query);
        this.simpleFeatureType=new GeoJSONToFeatureType(featureTypeUri,entry.getName().getNamespaceURI()).readType();
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
        boolean encrypt=((ImmuDBDataStore)getDataStore()).isEncryptFeatureType(getSchema());
        Filter[] split = splitFilter(query.getFilter(),encrypt);
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
            Object secKey=query.getHints().get(SECRET_KEY);
            Object iv=query.getHints().get(IV);
            reader=encrypt?new DecryptingImmuDBFeatureReader((ImmuDBDataStore) getDataStore(),getState(),querySchema,sql,params,secKey==null?null: secKey.toString(),iv==null?null:iv.toString()):new ImmuDBFeatureReader((ImmuDBDataStore) getDataStore(),getState(),querySchema,sql,params);
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

    protected Filter[] splitFilter(Filter original,boolean encrypted) {
        return splitFilter(original, this,encrypted);
    }

    Filter[] splitFilter(Filter original, SimpleFeatureSource source,boolean encrypt) {

        Filter[] split = new Filter[2];
        if (encrypt){
            split[0] = Filter.INCLUDE;
            split[1] = original;
        }else if (original != null) {
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
        return simpleFeatureType;
    }

    protected SQLValue[] getParams(ImmuDBFilterToSQL toSQL) throws IOException {
        SQLValue[] params = new SQLValue[toSQL.getLiteralValues().size()];
        List<Object> literals = toSQL.getLiteralValues();
        for (int i = 0; i < literals.size(); i++) {
            params[i] = Converter.getSQLValue(literals.get(i));
        }
        return params;
    }

    @Override
    protected boolean canFilter() {
        return true;
    }
}
