package mykidong;

import mykidong.util.Log4jConfigurer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

public class JdbcMetadataTestSkip {

    private static Logger log = LoggerFactory.getLogger(JdbcMetadataTestSkip.class);

    private DatabaseMetaData metadata;
    private Connection connection;

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

        connection = DriverManager.getConnection(url, properties);

        metadata = connection.getMetaData();
    }

    @Test
    public void getMetadataFromHiveViaJdbc() throws Exception
    {
        // run explicit query.
        ResultSet rs = connection.prepareStatement("describe formatted test.without_copying_file").executeQuery();
        while (rs.next())
        {
            String columnName = rs.getString(2);

            if(columnName.contains("Detailed Table Information"))
            {
                continue;
            }

            String dataType = rs.getString(1);

            log.info("column name: {}, data type: {} " + columnName, dataType);
        }
    }
}