//spark.sql("select * from test_parquet_table limit 10").show()
//
//spark.sql("select itemId, baseProperties.ts from test_parquet_table").show()


spark.sql("select * from test.parquet_as_delta3").show(200)