package org.geotools.immudb;

public class ImmuDBSessionParams {

    private String host;

    private int port;

    private String stateHolder;
    private String database;

    private String username;

    private String password;

    public ImmuDBSessionParams(String host, Integer port, String stateHolder,String database, String username, String password) {
        this.host=host;
        this.port=port;
        this.stateHolder=stateHolder;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getStateHolder() {
        return stateHolder;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
