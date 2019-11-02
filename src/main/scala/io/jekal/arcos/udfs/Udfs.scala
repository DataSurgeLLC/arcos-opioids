package io.jekal.arcos.udfs

import java.sql.Date

import io.jekal.arcos.StateMap
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object Udfs {
  def register(spark: SparkSession): Unit = {
    spark.sqlContext.udf.register("getStateCode", (state: String) => StateMap.stateCode(state))
    spark.sqlContext.udf.register("weekIndex", (date: Date) => {
      val referenceDate = new Date(2006 - 1900, 0, 1)
      val difference = (date.getTime - referenceDate.getTime)/86400000;
      difference / 7;
    })
    spark.sqlContext.udf.register("yearIndex", (date: Date) => {
      val referenceDate = new Date(2006 - 1900, 0, 1)
      date.getYear - referenceDate.getYear
    })
    spark.sqlContext.udf.register("getPopulation", (transactionDate: Date, population2006: Long, population2007: Long, population2008: Long, population2009: Long, population2010: Long, population2011: Long, population2012: Long) => {
      val populationsByYear = Array(population2006, population2007, population2008, population2009, population2010, population2011, population2012)
      val referenceDate = new Date(2006 - 1900, 0, 1)
      val index = transactionDate.getYear - referenceDate.getYear
      populationsByYear(index)
    })
    spark.sqlContext.udf.register("toVector", (array: Seq[Double]) => Vectors.dense(array.padTo(13, 0.0D).toArray))
  }
}
