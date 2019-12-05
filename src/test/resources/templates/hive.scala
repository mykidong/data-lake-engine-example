import java.util.{Map, Properties}

import com.cedarsoftware.util.io.JsonWriter
import com.fasterxml.jackson.databind.ObjectMapper
import mykidong.HiveJdbcMetadata

import scala.collection.JavaConversions._

val tableName = "test.without_copying_file"

// spark hive metastore for hive 1.2.x.
val url = "jdbc:hive2://mc-m01.opasnet.io:10016"

val properties = new Properties
properties.put("user", "hive")

val hiveJdbcMetadata = new HiveJdbcMetadata(url, properties)
val hiveMetadataMap = hiveJdbcMetadata.getMetadataFromHive(tableName)

val ddlMap = hiveMetadataMap.getDdlMap
val extraInfoMap = hiveMetadataMap.getExtraInfoMap

log.info("ddl: [" + JsonWriter.formatJson(new ObjectMapper().writeValueAsString(ddlMap)) + "]")
log.info("extra: [" + JsonWriter.formatJson(new ObjectMapper().writeValueAsString(extraInfoMap)) + "]")

var ddl = ""
var count = 0

for (columnName <- ddlMap.keySet) {
  if (count > 0) ddl += "," + columnName + " " + ddlMap.get(columnName)
  else ddl += columnName + " " + ddlMap.get(columnName)
  count += 1
}

val location = extraInfoMap.get("Location")

val newTableName = "test.without_copying_file_new"

var query = ""
query += "CREATE EXTERNAL TABLE IF NOT EXISTS " + newTableName + " ("
query += ddl
query += ")    "
query += "STORED AS PARQUET   "
query += "LOCATION '" + location + "'"

spark.sql("drop table if exists " + newTableName)
spark.sql(query)