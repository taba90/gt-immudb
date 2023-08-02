package org.geotools.jdbc.immudb;

import io.codenotary.immudb4j.ImmuClient;
import io.codenotary.immudb4j.sql.SQLException;
import org.geotools.data.Transaction;
import org.geotools.util.logging.Logging;

import java.io.IOException;
import java.util.logging.Logger;

public class ImmuDBTransactionState implements Transaction.State {

    private ImmuClient immuClient;

    private ImmuDBSessionParams sessionParams;
    private Transaction tx;

    private static Logger LOGGER= Logging.getLogger(ImmuDBTransactionState.class);
    public ImmuDBTransactionState(ImmuClient immuClient,ImmuDBSessionParams sessionParams,Transaction transaction){
        this.immuClient=immuClient;
        this.sessionParams=sessionParams;
        this.tx=transaction;
        immuClient.openSession(sessionParams.getDatabase(),sessionParams.getUsername(),sessionParams.getPassword());
        try {
            immuClient.beginTransaction();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setTransaction(Transaction tx) {
        if (tx != null && this.tx != null) {
            throw new IllegalStateException(
                    "New transaction set without " + "closing old transaction first.");
        }

        if (tx == null) {
            if (immuClient != null) {
                try {
                    immuClient.commitTransaction();
                    immuClient.closeSession();
                    immuClient.openSession(sessionParams.getDatabase(), sessionParams.getUsername(), sessionParams.getPassword());
                    immuClient.beginTransaction();
                } catch (SQLException e){
                    throw new RuntimeException(e);
                }
            } else {
                LOGGER
                        .warning(
                                "Transaction is attempting to "
                                        + "close an already closed connection");
            }
        }

        this.tx = tx;
    }

    @Override
    public void addAuthorization(String AuthID) throws IOException {}

    @Override
    public void commit() throws IOException {
            try {
                immuClient.commitTransaction();
            } catch (SQLException e) {
                String msg = "Error occured on commit";
                throw (IOException) new IOException(msg).initCause(e);
            }
    }

    @Override
    public void rollback() throws IOException {
            try {
                immuClient.rollbackTransaction();
            } catch (SQLException e) {
                String msg = "Error occured on rollback";
                throw (IOException) new IOException(msg).initCause(e);
            }
    }

    @Override
    @SuppressWarnings("deprecation") // finalize is deprecated in Java 9
    protected void finalize() throws Throwable {
        immuClient.closeSession();
    }
}
