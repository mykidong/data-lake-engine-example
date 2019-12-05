val lines = sc.textFile("/crawl/text/year=2019/month=12/day=04/hour=09/gyeonggido_category.json")
val lineLengths = lines.map(s => s.length)
val totalLength = lineLengths.reduce((a, b) => a + b)