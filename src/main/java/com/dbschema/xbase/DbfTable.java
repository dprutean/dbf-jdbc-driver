package com.dbschema.xbase;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static com.dbschema.xbase.DbfJdbcDriver.LOGGER;


/**
 * Copyright DbSchema@Wise Coders GmbH. All rights reserved.
 * Distribution of the code is prohibited. The code is free for use.
 */
class DbfTable {

    private final static char QUOTE_CHAR = '"';
    static final String META_TABLE_NAME = "dbs_meta_columns";
    private static final String DROP_META_TABLE =
            "drop table if exists " + META_TABLE_NAME;
    private static final String CREATE_META_TABLE =
            "create table if not exists " + META_TABLE_NAME + "( " +
                    "table_name varchar(2000) not null, " +
                    "column_name varchar(2000) not null, " +
                    "column_type varchar(120), " +
                    "length int not null, " +
                    "decimal int not null, " +
                    "primary key (table_name, column_name))";
    private static final String INSERT_INTO_META_TABLE =
            "insert into " + META_TABLE_NAME + "( table_name, column_name, column_type, length, decimal ) values ( ?,?,?,?,? )";

    final String name;
    private String insertSql;
    private String charset;
    private final List<DBFField> fields = new ArrayList<>();


   DbfTable( String name ){
        this.name = name;
    }

    DbfTable(File rootFolder, File tableFile) {
        String path = rootFolder.toURI().relativize(tableFile.toURI()).getPath();
        if ( path.toLowerCase().endsWith(".dbf")){
            path = path.substring(0, path.length() - ".dbf".length());
        }
        this.name = path;
    }

    void addField( DBFField field ){
        fields.add( field );
    }

    DBFField[] getDBFFields(){
        return fields.toArray( new DBFField[]{});
    }

    void load( Connection h2Connection, DBFReader dbfReader ) throws Exception {
        this.charset = ( dbfReader.getCharset() != null ) ? dbfReader.getCharset().name() : null;
        dropH2MetaTable( h2Connection );
        createH2MetaTable( h2Connection );
        transferDefinitionInH2( h2Connection, dbfReader );
        transferDataInH2( h2Connection, dbfReader);
    }

    private void transferDefinitionInH2(Connection h2Connection, DBFReader reader ) throws Exception {
        LOGGER.log(Level.INFO, "Transfer table '" + name + "'");
        final StringBuilder createSb = new StringBuilder("create table ").append(QUOTE_CHAR).append(name).append(QUOTE_CHAR).append("(\n");
        final StringBuilder insertSb = new StringBuilder("insert into ").append(QUOTE_CHAR).append(name).append(QUOTE_CHAR).append("(");
        final StringBuilder insertValuesSb = new StringBuilder("values(");
        boolean appendComma = false;
        int numberOfFields = reader.getFieldCount();
        for (int i = 0; i < numberOfFields; i++) {

            final DBFField field = reader.getField(i);
            saveFieldInMetaTable(h2Connection, field);
            fields.add(field);
            LOGGER.log(Level.INFO, "Column " + field );
            if (appendComma) {
                createSb.append(",\n");
                insertSb.append(",");
                insertValuesSb.append(",");
            }
            createSb.append("\t").append(QUOTE_CHAR).append(field.getName()).append(QUOTE_CHAR).append(" ");
            insertSb.append(QUOTE_CHAR).append(field.getName()).append(QUOTE_CHAR);
            insertValuesSb.append("?");
            createSb.append( FieldUtil.getH2Type( field));
            appendComma = true;
        }
        createSb.append(")");
        insertSb.append(")");
        insertValuesSb.append(")");

        String dropTableSQL = "drop table if exists " + QUOTE_CHAR + name + QUOTE_CHAR;
        LOGGER.log(Level.INFO, dropTableSQL);
        h2Connection.prepareStatement(dropTableSQL).execute();
        h2Connection.commit();


        LOGGER.log(Level.INFO, createSb.toString());
        h2Connection.prepareStatement(createSb.toString()).execute();
        h2Connection.commit();

        this.insertSql = insertSb.toString() + insertValuesSb.toString();
    }

     String getCharset(){
        return charset;
    }


    private void transferDataInH2(Connection h2Connection, DBFReader reader ) throws Exception {
        final PreparedStatement stInsert = h2Connection.prepareStatement(insertSql);
        Object[] record;
        while( ( record = reader.nextRecord()) != null ){

            for ( int i = 0; i < record.length && i < fields.size(); i++ ){
                Object value = record[i];
                DBFField field = fields.get( i );
                if (value != null) {
                    stInsert.setObject(i+1, value);
                } else {
                    stInsert.setNull(i+1, FieldUtil.getJavaType( field));
                }
            }
            LOGGER.log(Level.INFO, stInsert.toString());

            stInsert.execute();
            h2Connection.commit();
        }
    }


    private void createH2MetaTable( Connection h2Connection ) throws SQLException {
        final Statement st = h2Connection.createStatement();
        st.execute( CREATE_META_TABLE );
        st.close();
        h2Connection.commit();
    }

    private void dropH2MetaTable( Connection h2Connection ) throws SQLException {
        final Statement st = h2Connection.createStatement();
        st.execute( DROP_META_TABLE );
        st.close();
        h2Connection.commit();
    }

    private void saveFieldInMetaTable(Connection h2Connection, DBFField field) throws SQLException {
        final PreparedStatement st = h2Connection.prepareStatement(DbfTable.INSERT_INTO_META_TABLE);
        st.setString( 1, name);
        st.setString( 2, field.getName() );
        st.setString( 3, field.getType().name() );
        st.setInt( 4, field.getLength() );
        st.setInt( 5, field.getDecimalCount() );
        st.execute();
        h2Connection.commit();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name ).append("(\n");
        for ( DBFField field : fields ){
            sb.append( field).append(",\n");
        }
        sb.append(")");
        return sb.toString();
    }
}



