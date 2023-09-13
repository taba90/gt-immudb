package org.geotools.immudb;

import io.codenotary.immudb4j.FileImmuStateHolder;
import io.codenotary.immudb4j.ImmuStateHolder;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataUtilities;
import org.geotools.data.Parameter;
import org.geotools.util.SimpleInternationalString;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ImmuDBDataStoreFactory implements DataStoreFactorySpi {

    /** parameter for database host */
    public static final Param HOST = new Param("host", String.class, "Host", true, "localhost");

    /** parameter for database port */
    public static final Param PORT = new Param("port", Integer.class, "Port", true);

    /** parameter for database instance */
    public static final Param DATABASE = new Param("database", String.class, "Database", false);

    /** parameter for database user */
    public static final Param USER = new Param("user", String.class, "user name to login as");

    /** parameter for database password */
    public static final Param PASSWD =
            new Param(
                    "passwd",
                    String.class,
                    new SimpleInternationalString("password used to login"),
                    false,
                    null,
                    Collections.singletonMap(Parameter.IS_PASSWORD, Boolean.TRUE));

    /** parameter for namespace of the datastore */
    public static final Param NAMESPACE =
            new Param("namespace", String.class, "Namespace prefix", false);


    /** parameter for namespace of the datastore */
    public static final Param JSON_SCHEMA =
            new Param("jsonSchema", String.class, "JSON Schema URI", true);

    public static final Param STATE_HOLDER_PATH =
            new Param("stateHolderPath", String.class, "State Folder URI", true);


    @Override
    public String getDisplayName() {
        return getDescription();
    }

    @Override
    public String getDescription() {
        return "ImmuDB Data Store";
    }

    /**
     * Default implementation verifies the Map against the Param information.
     *
     * <p>It will ensure that:
     *
     * <ul>
     *   <li>params is not null
     *   <li>Everything is of the correct type (or upcovertable to the correct type without Error)
     *   <li>Required Parameters are present
     * </ul>
     *
     * @return true if params is in agreement with getParametersInfo and checkDBType
     */
    @Override
    public boolean canProcess(Map<String, ?> params) {
        if (!DataUtilities.canProcess(params, getParametersInfo())) {
            return false;
        }
        return true;
    }

    @Override
    public final ImmuDBDataStore createDataStore(Map<String, ?> params) throws IOException {
        String jsonSchema=(String) JSON_SCHEMA.lookUp(params);
        String stateFolder=(String) STATE_HOLDER_PATH.lookUp(params);
        ImmuStateHolder state= FileImmuStateHolder.newBuilder().withStatesFolder(stateFolder).build();
        String db=(String)DATABASE.lookUp(params);
        String host=(String) HOST.lookUp(params);
        Integer port = (Integer) PORT.lookUp(params);
        String username=(String) USER.lookUp(params);
        String pwd=(String)  PASSWD.lookUp(params);
        String ns=(String)  NAMESPACE.lookUp(params);
        try {
            ImmuDBSessionParams immuDBSessionParams=new ImmuDBSessionParams(host,port,stateFolder,db,username,pwd);
            return new ImmuDBDataStore(new URI(jsonSchema),ns,immuDBSessionParams);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ImmuDBDataStore createNewDataStore(Map<String, ?> params) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Param[] getParametersInfo() {
        Map<String, Object> map = new LinkedHashMap<>();
        setupParameters(map);

        return map.values().toArray(new Param[map.size()]);
    }
    protected void setupParameters(Map<String, Object> parameters) {
        parameters.put(HOST.key, HOST);
        parameters.put(PORT.key, PORT);
        parameters.put(DATABASE.key, DATABASE);
        parameters.put(USER.key, USER);
        parameters.put(PASSWD.key, PASSWD);
        parameters.put(NAMESPACE.key, NAMESPACE);
        parameters.put(STATE_HOLDER_PATH.key,STATE_HOLDER_PATH);
        parameters.put(JSON_SCHEMA.key,JSON_SCHEMA);
    }
    @Override
    public boolean isAvailable() {
        try {
            Class.forName("io.codenotary.immudb4j.ImmuClient");

            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Returns the implementation hints for the datastore.
     *
     * <p>Subclasses may override, this implementation returns <code>null</code>.
     */
    @Override
    public Map<java.awt.RenderingHints.Key, ?> getImplementationHints() {
        return null;
    }
}
