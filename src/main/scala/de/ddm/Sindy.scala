package de.ddm

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    var pipeline = spark.emptyDataset[(String, Set[String])].rdd
    for (input <- inputs.indices) {
      val dataFromFile = readData(inputs(input), spark)
      val pipe = dataFromFile.flatMap(row => {
        val list = new Array[(String, Set[String])](row.size)
        for (value <- row.schema.indices) {
          list(value) = (row.getString(value), Set(row.schema.fieldNames(value)))
        }
        list
      })
        .rdd
        .distinct()
      pipeline = pipeline.union(pipe)
    }
    val result = pipeline
      .reduceByKey((v1, v2) => v1.union(v2))
      .flatMap(v => {
        val list = new Array[(String, Set[String])](v._2.size)
        var i = 0
        for (value <- v._2) {
          list(i) = (value, v._2.filterNot(_ == value))
          i += 1
        }
        list
      })
      .reduceByKey((v1, v2) => v1.intersect(v2))
      .filter(v => v._2.nonEmpty)
      .sortBy(v => v._1)
      .collect()
    for (dep <- result) {
      println(s"${dep._1} < ${dep._2.mkString("", ",", "")}")
    }
  }

}
