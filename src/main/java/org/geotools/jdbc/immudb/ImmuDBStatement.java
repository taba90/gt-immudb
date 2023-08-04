package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.ImmuClient;
import io.codenotary.immudb4j.sql.SQLException;
import io.codenotary.immudb4j.sql.SQLQueryResult;
import io.codenotary.immudb4j.sql.SQLValue;

import java.io.IOException;

public class ImmuDBStatement {

    private String sql;

    private ImmuClient immuClient;
    private SQLValue[] params;

    public ImmuDBStatement(String sql, ImmuClient immuClient,SQLValue[] params) {
        this.sql = sql;
        this.immuClient = immuClient;
        this.params=params;
    }

    public ImmuDBStatement(String sql, ImmuClient immuClient) {
        this.sql = sql;
        this.immuClient = immuClient;
    }

    public SQLQueryResult executeQuery(SQLValue[] params) throws SQLException {
        return immuClient.sqlQuery(sql,params);
    }

    public SQLQueryResult executeQuery() throws IOException {
        try {
            return immuClient.sqlQuery(sql,params);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public void executeStmt(SQLValue[] params) throws IOException {
        try {
            immuClient.sqlExec(sql,params);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public ImmuClient getImmuClient() {
        return immuClient;
    }
}
