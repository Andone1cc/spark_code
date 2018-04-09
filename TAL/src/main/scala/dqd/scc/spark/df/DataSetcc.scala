package dqd.scc.spark.df

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

/**
  * Created by andone1cc on 2018/3/16.
  */
object DataSetcc {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local")
      .appName("DataSet").getOrCreate()

    import spark.implicits._


    val caseClassDS = Seq(Person("Andy", 32)).toDS()

    caseClassDS.show()

    val primitiveDS: Dataset[Int] = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).show()

    val peopleDS: Dataset[Person] = spark.read.json("jsonfile").as[Person]
    peopleDS.show()


    val peopleDF: DataFrame = spark.sparkContext.textFile("people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("select name,age from people where age between 13 and 19")

    teenagersDF.map(teenager => "Name:" + teenager(0)).show()

    teenagersDF.map(teenager => "Name:" + teenager.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()

  }
}
