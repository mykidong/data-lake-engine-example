import org.apache.spark.sql.SparkSession


// NOTE: variable 'spark' 가 이미 REPL 에 SparkSession instance 로 생성되었기 때문에
//       개발할때만 uncomment 하고 commit 할 경우는 comment 시킴.
//val spark: SparkSession

// TODO: 사용자 Request 별 Session 을 생성해야 하나....
// create new spark session.
val newSpark = spark.newSession

val slices = 10
val numberOfThrows = 100000 * slices
System.out.println("About to throw " + numberOfThrows + " darts, ready? Stay away from the target!")

val t0 = System.currentTimeMillis

import org.apache.spark.sql.Encoders

val t1 = System.currentTimeMillis
System.out.println("Session initialized in " + (t1 - t0) + " ms")

var l: List[Integer] = List()

for(i <- 0 to (numberOfThrows - 1))
{
    l = l:+i.asInstanceOf[Integer]
}

val incrementalDf = newSpark.createDataset(l)(Encoders.INT)

val t2 = System.currentTimeMillis
System.out.println("Initial dataframe built in " + (t2 - t1) + " ms")

var counter = 0

val dotsDs = incrementalDf.map(i => {
  val x = Math.random * 2 - 1
  val y = Math.random * 2 - 1
  counter += 1
  if (counter % 100000 == 0)
    System.out.println("" + counter + " darts thrown so far")
  if (x * x + y * y <= 1)
    1.asInstanceOf[Integer]
  else
    0.asInstanceOf[Integer]
})(Encoders.INT)

val t3 = System.currentTimeMillis
System.out.println("Throwing darts done in " + (t3 - t2) + " ms")

val dartsInCircle = dotsDs.reduce((x, y) => x + y)
val t4 = System.currentTimeMillis
System.out.println("Analyzing result in " + (t4 - t3) + " ms")

System.out.println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows)


