val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
val sum = distData.reduce((a, b) => (a + b))