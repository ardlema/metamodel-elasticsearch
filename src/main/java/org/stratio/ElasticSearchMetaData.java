package org.stratio;

import org.apache.metamodel.schema.ColumnType;

public class ElasticSearchMetaData {
    private String[] columnNames;
    private ColumnType[] columnTypes;

    public ElasticSearchMetaData(String[] names, ColumnType[] types) {
        this.columnNames = names;
        this.columnTypes = types;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public ColumnType[] getColumnTypes() {
        return columnTypes;
    }
}
