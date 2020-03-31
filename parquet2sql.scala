// One way Transforming via SPARK
// Any by Python, Scala, Java.


// Writing to Parquet: Flat Schema..
//     Customize transforming Rows:
def transformRow(r: Row):   ZeppelinFlat = {
    def getStr(r: Row, i: Int) = if(!r.isNullAt(i)) some(r.getString(i)) esle None
    def getInt(r: Row, i: Int) = if(!r.isNullAt(i)) some(r.getInt(i)) esle None

val outDf = spark.read
                 .option("delimiter", "\t")
                 .option("header", "true")
                 .csv(flatInput).rdd
                 .map(r => transformRow(r))
                 .toDF

outDF.write
     .option("compression", "snappy"(
     .parquet(flatOutput)


// Writing yo Parquet: Nested Schema!

val nestDf = sparj.read.json(nestInput)

nestDf.write
      .option("compression", "snappy")
      .parquet(nestOutput)


// One way via Spark!

val flatParquet = "s3a://path/ZeppelinFlatSchema.parquet"
val flatdf = spark.read.parquet(flatParquet)
flatdf.createOrReplaceTempView("ZeppelinFlat")

val nestParquet = "s3a://path/ZeppelinNestSchema.parquet")
val nestdf = spark.read.parquet(nestParquet)
nestdf.createOrReplaceTempView("ZeppelinNest")

val flatQuery = "select Title, US from ZeppelinFlat where US = 1"
val nestQuery = "select Title, PeakChart.US from ZeppelinNest where PeakChart.US = 1"

spark.sql(flatQuery)
spark.sql(nestQuery)
