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

            if(columnName == null || dataType == null)
            {
                continue;
            }

            if(!columnName.trim().equals("") && !columnName.trim().contains("#") && isDDL)
            {
                ddlMap.put(columnName, dataType);

                log.info("in ddlmap, columnName: [" + columnName + "], dataType: [" + dataType + "]");

                continue;
            }


            if(columnName.trim().contains("#") && (columnName.trim().contains("Detailed Table Information") || columnName.trim().contains("Storage Information")))
            {
                isDDL = false;
                continue;
            }

            if(!columnName.trim().equals("") && !columnName.trim().contains("#") && !isDDL)
            {
                extraInfoMap.put(columnName, dataType);
                log.info("in extramap, columnName: [" + columnName + "], dataType: [" + dataType + "]");
            }
        }

        HiveMetadataMap hiveMetadataMap = new HiveMetadataMap();
        hiveMetadataMap.setDdlMap(ddlMap);
        hiveMetadataMap.setExtraInfoMap(extraInfoMap);

        return hiveMetadataMap;
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
