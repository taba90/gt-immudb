package org.geotools.jdbc.immudb;

public class ImmuDBSessionParams {

    private String database;

    private String username;

    private String password;

    public ImmuDBSessionParams(String database, String username, String password) {
        this.database = database;
        this.username = username;
        this.password = password;
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
