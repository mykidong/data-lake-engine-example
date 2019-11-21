package mykidong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HiveJdbcMetadata {

    private static Logger log = LoggerFactory.getLogger(HiveJdbcMetadata.class);

    private DatabaseMetaData metadata;
    private Connection connection;

    public HiveJdbcMetadata(String url, Properties properties)
    {
        try {
            connection = DriverManager.getConnection(url, properties);

            metadata = connection.getMetaData();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    public HiveMetadataMap getMetadataFromHive(String tableWithDatabase) throws Exception
    {
        // run explicit query.
        ResultSet rs = connection.prepareStatement("describe formatted " + tableWithDatabase).executeQuery();

        Map<String, String> ddlMap = new HashMap<>();
        Map<String, String> extraInfoMap = new HashMap<>();

        boolean isDDL = true;

        while (rs.next())
        {
            String columnName = rs.getString(1);
            String dataType = rs.getString(2);

            if(columnName == null || dataType == null || columnName.trim().equals(""))
            {
                continue;
            }

            String column = columnName.trim();

            if(!column.startsWith("#") && !column.contains(":") && isDDL)
            {
                ddlMap.put(column, dataType);

                log.info("in ddlmap, columnName: [" + column + "], dataType: [" + dataType + "]");
            }


            if(column.contains("Detailed Table Information"))
            {
                isDDL = false;
                log.info("isDDL set to " + isDDL);

                continue;
            }
            else if(column.contains(":"))
            {
                isDDL = false;
                log.info("isDDL set to " + isDDL);

                // remove semicolon.
                int index = column.indexOf(":");
                if(index > 0)
                {
                    column = column.substring(0, index);
                }

                extraInfoMap.put(column, dataType.trim());
                log.info("in extramap, columnName: [" + column + "], dataType: [" + dataType + "]");
            }

            if(!column.startsWith("#") && !isDDL)
            {
                extraInfoMap.put(column, dataType.trim());
                log.info("in extramap, columnName: [" + column + "], dataType: [" + dataType + "]");
            }
        }

        HiveMetadataMap hiveMetadataMap = new HiveMetadataMap();
        hiveMetadataMap.setDdlMap(ddlMap);
        hiveMetadataMap.setExtraInfoMap(extraInfoMap);

        return hiveMetadataMap;
    }


    public String getCreateTableDDLFromHive(String tableWithDatabase) throws Exception
    {
        String query = "show create table " + tableWithDatabase;

        // run explicit query.
        ResultSet rs = connection.prepareStatement(query).executeQuery();

        String ddl = null;
        while (rs.next()) {
            ddl = rs.getString(1);
            break;
        }

        return ddl;
    }

    public static class HiveMetadataMap
    {
        private Map<String, String> ddlMap;
        private Map<String, String> extraInfoMap;

        public Map<String, String> getDdlMap() {
            return ddlMap;
        }

        public void setDdlMap(Map<String, String> ddlMap) {
            this.ddlMap = ddlMap;
        }

        public Map<String, String> getExtraInfoMap() {
            return extraInfoMap;
        }

        public void setExtraInfoMap(Map<String, String> extraInfoMap) {
            this.extraInfoMap = extraInfoMap;
        }
    }
}
