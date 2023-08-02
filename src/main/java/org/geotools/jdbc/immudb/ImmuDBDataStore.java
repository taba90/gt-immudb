package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.ImmuClient;
import io.codenotary.immudb4j.ImmuState;
import io.codenotary.immudb4j.ImmuStateHolder;
import io.codenotary.immudb4j.sql.SQLException;
import io.codenotary.immudb4j.sql.SQLQueryResult;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.data.store.ContentState;
import org.geotools.feature.NameImpl;
import org.geotools.jdbc.JDBCState;
import org.geotools.jdbc.NullPrimaryKey;
import org.geotools.jdbc.PrimaryKey;
import org.geotools.jdbc.PrimaryKeyColumn;
import org.geotools.util.Converters;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ImmuDBDataStore extends ContentDataStore {

    private ImmuClient immuClient;

    private ImmuDBSessionParams immuDBSessionParams;
    private URI featureTypeUri;
    private Logger LOGGER = Logging.getLogger(ImmuDBDataStore.class);
    public ImmuDBDataStore(URI featureTypeUri, String ns, String host, Integer port, ImmuDBSessionParams immuDBSessionParams, ImmuStateHolder stateHolder){
        this.immuClient=ImmuClient.newBuilder()
                .withServerUrl(host)
                .withServerPort(port)
                .withStateHolder(stateHolder)
                .build();
        this.immuDBSessionParams=immuDBSessionParams;
        setNamespaceURI(ns);
        this.featureTypeUri=featureTypeUri;
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        List<Name> names=new ArrayList<>();
        open(Transaction.AUTO_COMMIT);
        try {
            SQLQueryResult queryResult=immuClient.sqlQuery("SELECT name from TABLES()");
            while (queryResult.next()){
                String tname=queryResult.getString(0);
                names.add(new NameImpl(namespaceURI,tname));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            releaseConnection(Transaction.AUTO_COMMIT);
        }
        return names;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new ImmuDBFeatureStore(featureTypeUri,immuDBSessionParams,immuClient,entry,Query.ALL);
    }

    public void open(Transaction transaction) {
        ImmuDBTransactionState state=new ImmuDBTransactionState(immuClient,immuDBSessionParams,transaction);
        if (!transaction.equals(Transaction.AUTO_COMMIT)) transaction.putState(this,state);
    }

    public final void releaseConnection(Transaction transaction) {
        if (transaction == Transaction.AUTO_COMMIT) {
            try {
                immuClient.commitTransaction();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            immuClient.closeSession();
        }
    }

    @Override
    public void dispose() {
        super.dispose();
        try {
            immuClient.shutdown();
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE,"Error while disposing the ImmuDB datastore",e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    ImmuDBFilterToSQL filter(
            SimpleFeatureType featureType, Filter filter, StringBuffer sql, ImmuDBFilterToSQL toSQL)
            throws IOException {

        try {
            // grab the full feature type, as we might be encoding a filter
            // that uses attributes that aren't returned in the results
            toSQL.setInline(true);

            String filterSql = toSQL.encodeToString(filter);
            sql.append(filterSql);
            return toSQL;
        } catch (FilterToSQLException e) {
            throw new RuntimeException(e);
        }
    }

    private FilterToSQL getFilterToSQL(StringWriter writer) {
        return new ImmuDBFilterToSQL(writer);
    }

    /** Encodes the sort-by portion of an sql query */
    void sort(SimpleFeatureType featureType, SortBy[] sort, String prefix, StringBuffer sql)
            throws IOException {
        if ((sort != null) && (sort.length > 0)) {
            String key = getPrimaryKey(featureType);
            sql.append(" ORDER BY ");

            for (SortBy sortBy : sort) {
                String order;
                if (sortBy.getSortOrder() == SortOrder.DESCENDING) {
                    order = " DESC";
                } else {
                    order = " ASC";
                }

                if (SortBy.NATURAL_ORDER.equals(sortBy) || SortBy.REVERSE_ORDER.equals(sortBy)) {
                    sql.append(key).append(" ");
                } else {
                    sql.append(sortBy).append(" ");
                }
                sql.append(order);
                sql.append(",");
            }

            sql.setLength(sql.length() - 1);
        }
    }


    void selectColumns(SimpleFeatureType featureType, String prefix, Query query, StringBuffer sql)
            throws IOException {

        // primary key
        String key = null;
        try {
            key = getPrimaryKey(featureType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // we need to add the primary key columns only if they are not already exposed
            sql.append(key);
            sql.append(",");

        // other columns
        for (AttributeDescriptor att : featureType.getAttributeDescriptors()) {
            String columnName = att.getLocalName();
            // skip the eventually exposed pk column values
            if (key.equalsIgnoreCase(columnName)) continue;
            sql.append(att.getLocalName());
            sql.append(",");
        }
    }

    /**
     * Generates a 'SELECT p1, p2, ... FROM ... WHERE ...' prepared statement.
     *
     * @param featureType the feature type that the query must return (may contain less attributes
     *     than the native one)
     * @param query the query to be run. The type name and property will be ignored, as they are
     *     supposed to have been already embedded into the provided feature type
     */
    protected String selectSQLPS(
            SimpleFeatureType featureType, ImmuDBFilterToSQL toSQL, Query query)
            throws SQLException, IOException {

        StringBuffer sql = new StringBuffer();
        sql.append("SELECT ");

        // column names
        selectColumns(featureType, null, query, sql);
        sql.setLength(sql.length() - 1);

        sql.append(" FROM ").append(featureType.getTypeName());
        // filtering
        Filter filter = query.getFilter();
        if (filter != null && !Filter.INCLUDE.equals(filter)) {
            sql.append(" WHERE ");

            // encode filter
             filter(featureType, filter, sql,toSQL);
        }

        // sorting
        sort(featureType, query.getSortBy(), null, sql);

        // finally encode limit/offset, if necessary
        applyLimitOffset(sql, query.getStartIndex(), query.getMaxFeatures());

        // add search hints if the dialect supports the
        LOGGER.fine(sql.toString());


        return sql.toString();
    }

    void applyLimitOffset(StringBuffer sql, final Integer offset, final int limit) {
        if (checkLimitOffset(offset, limit)) {

        }
    }

    /**
     * Applies the limit/offset elements to the query if they are specified and if the dialect
     * supports them
     *
     * @param sql The sql to be modified
     * @param query the query that holds the limit and offset parameters
     */
    public void applyLimitOffset(StringBuffer sql, Query query) {
        applyLimitOffset(sql, query.getStartIndex(), query.getMaxFeatures());
    }

    /**
     * Checks if the query needs limit/offset treatment
     *
     * @return true if the query needs limit/offset treatment and if the sql dialect can do that
     *     natively
     */
    boolean checkLimitOffset(final Integer offset, final int limit) {
        // if we cannot, don't bother checking the query
        return limit != Integer.MAX_VALUE || (offset != null && offset > 0);
    }

    /**
     * Returns the primary key object for a particular feature type / table, deriving it from the
     * underlying database metadata.
     */
    public String getPrimaryKey(SimpleFeatureType featureType) throws IOException {
        return getPrimaryKey(ensureEntry(featureType.getName()));
    }

    protected String getPrimaryKey(ContentEntry entry) throws IOException {
        ImmuDBState state = (ImmuDBState) entry.getState(Transaction.AUTO_COMMIT);

        if (state.getPrimaryKey() == null) {
            synchronized (this) {
                if (state.getPrimaryKey() == null) {
                    // get metadata from database
                    try {
                        String pk=(String)state.getFeatureType().getUserData().get(GeoJSONToFeatureType.PK_USER_DATA);
                        state.setPrimaryKey(pk);
                    } catch (Exception e) {
                        String msg = "Error looking up primary key";
                        throw new IOException(msg, e);
                    }
                }
            }
        }

        return state.getPrimaryKey();
    }


    protected static LinkedHashSet<String> getColumnNames(PrimaryKey key) {
        LinkedHashSet<String> pkColumnNames = new LinkedHashSet<>();
        for (PrimaryKeyColumn pkcol : key.getColumns()) {
            pkColumnNames.add(pkcol.getName());
        }
        return pkColumnNames;
    }


    public static List<Object> decodeFID(PrimaryKey key, String FID, boolean strict) {
        // strip off the feature type name
        if (FID.startsWith(key.getTableName() + ".")) {
            FID = FID.substring(key.getTableName().length() + 1);
        }

        try {
            FID = URLDecoder.decode(FID, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        // check for case of multi column primary key and try to backwards map using
        // "." as a seperator of values
        List<Object> values = null;
        if (key.getColumns().size() > 1) {
            String[] split = FID.split("\\.");

            // copy over to avoid array store exception
            values = new ArrayList<>(split.length);
            for (String s : split) {
                values.add(s);
            }
        } else {
            // single value case
            values = new ArrayList<>();
            values.add(FID);
        }
        if (values.size() != key.getColumns().size()) {
            throw new IllegalArgumentException(
                    "Illegal fid: "
                            + FID
                            + ". Expected "
                            + key.getColumns().size()
                            + " values but got "
                            + values.size());
        }

        // convert to the type of the key
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value != null) {
                Class<?> type = key.getColumns().get(i).getType();
                Object converted = Converters.convert(value, type);
                if (converted != null) {
                    values.set(i, converted);
                }
                if (strict && !type.isInstance(values.get(i))) {
                    throw new IllegalArgumentException(
                            "Value " + values.get(i) + " illegal for type " + type.getName());
                }
            }
        }

        return values;
    }

    @Override
    protected ContentState createContentState(ContentEntry entry) {
        return new ImmuDBState(entry);
    }
}
