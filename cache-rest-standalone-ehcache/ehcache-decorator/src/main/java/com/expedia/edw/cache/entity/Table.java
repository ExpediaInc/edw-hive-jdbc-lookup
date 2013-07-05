package com.expedia.edw.cache.entity;

/**
 *
 * @author Yaroslav_Mykhaylov
 */
public class Table {

    private String schemaName;
    private String tableName;
    private String keyName;
    private String valueName;

    public Table(String schemaName, String tableName, String keyName, String valueName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.keyName = keyName;
        this.valueName = valueName;
    }

    public Table() {
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getKeyName() {
        return keyName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public String getValueName() {
        return valueName;
    }

    public void setValueName(String valueName) {
        this.valueName = valueName;
    }

    public String toString() {
        return "[" + this.schemaName + "]"
                + "[" + this.tableName + "]"
                + "[" + this.keyName + "]"
                + "[" + this.valueName + "]";
    }
}
