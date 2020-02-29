val lines = sc.textFile("/crawl/text/year=2020/month=01/day=18/hour=08/seoulcity_category_42_2.json")
val lineLengths = lines.map(s => s.length)
val totalLength = lineLengths.reduce((a, b) => a + b)