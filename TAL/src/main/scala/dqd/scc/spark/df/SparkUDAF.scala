package dqd.scc.spark.df

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * Created by andone1cc on 2018/3/19.
  */
object SparkUDAF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      .appName("SparkUDAF")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read.json("udaf")
    df.createOrReplaceTempView("inventory")
    spark.udf.register("SUMPRODUCT", new SumProductAggregateFunction)

    spark.sql("select Make, SUMPRODUCT(RetailValue,Stock) as InventoryValuePerMake from inventory group by Make").show()
  }

  private class SumProductAggregateFunction extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = new StructType().add("price", DoubleType).add("quantity", LongType)

    override def bufferSchema: StructType = new StructType().add("total", DoubleType)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0.0)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      val price = input.getDouble(0)
      val qty = input.getLong(1)
      buffer.update(0, sum + (price * qty))
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  }

}
