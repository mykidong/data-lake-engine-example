package mykidong.connector.hive;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.immutable.Map;

public class HiveRelationProvider implements RelationProvider, DataSourceRegister {

    @Override
    public String shortName() {
        return "hive";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        return null;
    }
}
