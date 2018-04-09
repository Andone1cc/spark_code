package dqd.scc.spark.df

import org.apache.spark.sql.SparkSession

/**
  * Created by andone1cc on 2018/3/16.
  */
object DataFramecc {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("jsonfile")

    df.show()

    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()

    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()
    spark.newSession().sql("select * from global_temp.people").show()
  }
}

