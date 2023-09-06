package org.geotools.jdbc.immudb;

import org.geotools.jdbc.AutoGeneratedPrimaryKeyColumn;
import org.geotools.jdbc.PrimaryKey;
import org.geotools.jdbc.PrimaryKeyColumn;

import java.util.Arrays;
import java.util.List;

public class ImmuDBPk extends PrimaryKey {

    public ImmuDBPk(String typeName, String pkColumn,Class<?> type){
        this(typeName, Arrays.asList(new AutoGeneratedPrimaryKeyColumn(pkColumn,type)));
    }
    private ImmuDBPk(String tableName, List<PrimaryKeyColumn> columns) {
        super(tableName, columns);
    }
}