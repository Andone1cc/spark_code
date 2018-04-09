package dqd.scc.spark.df

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by andone1cc on 2018/3/19.
  */
object SparkUDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df: DataFrame = spark.read.json("te")

    df.createOrReplaceTempView("citytemps")

    spark.udf.register("CTOF", (degreesCelcius: Double) => ((degreesCelcius * 9.0 / 5.0) + 32.0))

    spark.sql("select city,CTOF(avgLow) as avgLowF,CTOF(avgHigh) as avgHighF from citytemps").show()
  }
}
