package mykidong;

import com.fasterxml.jackson.databind.ObjectMapper;
import mykidong.util.Log4jConfigurer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class JdbcMetadataTestSkip {

    private static Logger log = LoggerFactory.getLogger(JdbcMetadataTestSkip.class);

    private DatabaseMetaData metadata;

    @Before
    public void init() throws Exception
    {
        // init. log4j.
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j.xml");
        log4j.afterPropertiesSet();

        // URL parameters
        String url = "jdbc:hive2://mc-m01.opasnet.io:10016";

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        Connection connection = DriverManager.getConnection(url, properties);

        metadata = connection.getMetaData();
    }

    @Test
    public void getMetadataFromHiveViaJdbc() throws Exception
    {
        printGeneralMetadata();
        getColumnsMetadata(getTablesMetadata());    }


    private void printGeneralMetadata() throws Exception {
        log.info("Database Product Name: "
                + metadata.getDatabaseProductName());
        log.info("Database Product Version: "
                + metadata.getDatabaseProductVersion());
        
        log.info("JDBC Driver: " + metadata.getDriverName());
        log.info("Driver Version: " + metadata.getDriverVersion());
    }

    private ArrayList<String> getTablesMetadata() throws Exception {
        String table[] = { "TABLE" };
        ResultSet rs = null;
        ArrayList<String> tables = null;
        // receive the Type of the object in a String array.
        rs = metadata.getTables(null, null, null, table);

        log.info("rs: " + new ObjectMapper().writeValueAsString(rs));

        tables = new ArrayList();
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }

    private void getColumnsMetadata(ArrayList<String> tables)
            throws Exception {
        ResultSet rs = null;
        // Print the columns properties of the actual table
        for (String actualTable : tables) {
            rs = metadata.getColumns(null, null, actualTable, null);
            log.info(actualTable.toUpperCase());
            while (rs.next()) {
                log.info(rs.getString("COLUMN_NAME") + " "
                        + rs.getString("TYPE_NAME") + " "
                        + rs.getString("COLUMN_SIZE"));
            }
            log.info("\n");
        }

    }
}
