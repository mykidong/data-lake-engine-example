spark.sql("select * from test_parquet_table limit 10").show()

spark.sql("select itemId, baseProperties.ts from test_parquet_table").show()