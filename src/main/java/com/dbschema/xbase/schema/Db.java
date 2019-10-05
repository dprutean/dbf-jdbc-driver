package com.dbschema.xbase.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Db {

    private final Map<String, Table> tables = new HashMap<>();

    public Table getOrCreateTable(String tableName ){
        if ( tables.containsKey( tableName )){
            return tables.get(tableName);
        } else {
            Table table = new Table(tableName);
            tables.put( tableName, table);
            return table;
        }
    }

    public Collection<Table> getTables(){
        return tables.values();
    }

}
