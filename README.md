# DBF-JDBC-DRIVER
DBF-JDBC-DRIVER is a JDBC driver for xBase DBF files.

The code is provided by [DbSchema Database Designer: www.dbschema.com](https://www.dbschema.com) by [Wise Coders GmbH](https://www.wisecoders.com)
The driver can be tested by downloading and installing DbSchema. 
There is no need to register or download the driver - DbSchema will do everything for you.
DbSchema can be tested 15 days for free.

For any question please contact support at dbschema.com

The driver loads all DBF data into an H2 database stored in <user.home>/.DbSchema/dbf-jdbc-driver/
The data can be modified, any kind of queries can be executed.
To save back data to dbf files use:

```
Statement st = connection.createStatement();
st.execute("save dbf to <folder_path>");
```
We save data to a different path, just to avoid any storage issues and don't loose any information.
We appreciate any contributions to this repository.

## License

BSD License-3. Free to use, distribution forbidden. Improvements of the driver accepted only in https://bitbucket.org/dbschema/dbf-jdbc-driver.


# JDBC URL

jdbc:dbschema:dbf:<folder-with-dbf-files>?[charset=ISO_8859_1]


```
Connection con = DriverManager.getConnection( "jdbc:dbschema:dbf:/sample_dbf_folder" );
Statement st = con.createStatement();
ResultSet rs = st.executeQuery("select * from cars")
while( rs.next() ){
    ....
}
```

# News 

Version 1.0 is released.