package com.dbschema.xbase;


import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import static com.dbschema.xbase.DbfJdbcDriver.LOGGER;

/**
 * Copyright DbSchema@Wise Coders GmbH. All rights reserved.
 * Distribution of the code is prohibited. The code is free for use.
 */
class DbfStore {


    private final Map<String,DbfTable> tables = new HashMap<>();


    private void storeField(String tableName, DBFField field ){
        if ( tables.containsKey( tableName )){
            tables.get(tableName).addField( field );
        } else {
            DbfTable table = new DbfTable(tableName);
            table.addField( field );
            tables.put( tableName, table);
        }
    }

    DbfStore(Connection h2Connection, File outputFolder, String charset ) throws Exception {

        final ResultSet rsColumns = h2Connection.getMetaData().getColumns( null, null, null, null );
        while( rsColumns.next() ){
            String tableName = rsColumns.getString( 3 );
            if ( !FieldUtil.isH2SystemTable(tableName )) {
                String columnName = rsColumns.getString(4);
                LOGGER.info("Define column " + tableName + "." + columnName);
                DBFField field = FieldUtil.getDBFField(columnName, rsColumns.getString(6), rsColumns.getInt(7), rsColumns.getInt(9));
                storeField(tableName, field);
            }
        }

        for ( DbfTable table : tables.values() ){
            final File outputFile = new File( outputFolder.toURI().resolve( table.name + ".dbf"));
            LOGGER.info("Storing " + table + "...");

            final FileOutputStream os = new FileOutputStream(outputFile);
            final DBFWriter writer = charset != null ? new DBFWriter(os, Charset.forName(charset)) : new DBFWriter(os);
            writer.setFields( table.getDBFFields() );

            try ( Statement st = h2Connection.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT * FROM " + table.name);
                int recCount = 0;
                while (rs.next()) {
                    int columnCount = rs.getMetaData().getColumnCount();
                    Object[] data = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        data[i] = rs.getObject(i + 1);
                    }
                    try {
                        writer.addRecord(data);
                    } catch ( Throwable ex ){
                        StringBuilder sb = new StringBuilder();
                        sb.append("Error saving ").append( outputFile.getAbsolutePath() ).append( " record : [");
                        for ( Object obj: data){
                            if ( obj == null ){
                                sb.append("null");
                            } else {
                                sb.append("'").append( obj.toString()).append("'");
                            }
                            sb.append(",");
                        }
                        sb.append(" ]");
                        throw new SQLException(sb.toString() + ex.getLocalizedMessage(), ex );
                    }
                    recCount++;
                }
                rs.close();
                writer.close();
                LOGGER.info("Stored " + table.name + " " + recCount + " records." );
            }
        }
    }




}
