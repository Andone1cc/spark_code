package dqd.scc.spark.df


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by andone1cc on 2018/3/16.
  */
object DataSetcc2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local")
      .appName("DataSetcc2").getOrCreate()

    import spark.implicits._

    val peopleRDD: RDD[String] = spark.sparkContext.textFile("people.txt")

    val schemaString = "name age"

    val fields: Array[StructField] = schemaString.split(" ")
      .map(filedName => StructField(filedName, StringType, nullable = true))

    val schema: StructType = StructType(fields)

    val rowRDD: RDD[Row] = peopleRDD.map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.createOrReplaceTempView("people")

    val results: DataFrame = spark.sql("select name from people")

    results.map(attributes => "Name: " + attributes(0)).show()


    val peoDF = spark.read.format("json").load("jsonfile")
    peoDF.select("name","age").write.format("parquet").save("people.parquet")

  }
}
