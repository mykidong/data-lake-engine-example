val parquetDs = spark.read.format("parquet").load("/test-event-parquet");
parquetDs.show(3);
val sum: Long = parquetDs.map(row => {  println("row: " + row.toString);  1 }).count();
println("sum: [" + sum + "]");