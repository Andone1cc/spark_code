package dqd.scc.spark.df

import org.apache.spark.sql.SparkSession

/**
  * Created by andone1cc on 2018/3/16.
  */
object DF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local")
      .appName("DF").getOrCreate()

    import spark.implicits._

    val peopleDF = spark.read.json("jsonfile")
    //peopleDF.write.parquet("people.parquet")

    val parquetFileDF = spark.read.parquet("people.parquet")

    parquetFileDF.createOrReplaceTempView("parquetFile")

    val namesDF = spark.sql("select name from parquetFile where age between 13 and 19")

    namesDF.map(attributes => "Name:" + attributes(0)).show()


    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")

    squaresDF.write.parquet("test/key=1")

    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")

    cubesDF.write.parquet("test/key=2")

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("test")

    mergedDF.printSchema()
  }
}
