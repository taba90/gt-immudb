package org.geotools.jdbc.immudb;

import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentState;
import org.geotools.jdbc.PrimaryKey;

public class ImmuDBState extends ContentState {

    private String primaryKey;

    /** Creates the state from an existing one. */
    public ImmuDBState(ImmuDBState state) {
        super(state);

        // copy the primary key
        primaryKey = state.getPrimaryKey();
    }

    /** Creates a new state object. */
    public ImmuDBState(ContentEntry entry) {
        super(entry);
    }

    /** The cached primary key. */
    public String getPrimaryKey() {
        return primaryKey;
    }
    /** Sets the cached primary key. */
    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    /** Flushes all cached state. */
    @Override
    public void flush() {
        primaryKey = null;
        super.flush();
    }

    /** Copies the state. */
    @Override
    public ContentState copy() {
        return new ImmuDBState(this);
    }
}
