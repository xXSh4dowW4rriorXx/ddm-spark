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
    val names = new Array[Array[String]](inputs.size)
    val fileIndices = new Array[Broadcast[Int]](inputs.size)
    var pipeline = spark.emptyDataset[(String, Set[(Int, Int)])].rdd
    for (input <- inputs.indices) {
      val bcInput = spark.sparkContext.broadcast(input)
      fileIndices(input) = bcInput
      val dataFromFile = readData(inputs(input), spark)
      names(input) = dataFromFile.schema.fieldNames

      val pipe = dataFromFile.flatMap(row => {
        val list = new Array[(String, Set[(Int, Int)])](row.size)
        for (value <- 0 until row.size) {
          list(value) = (row.getString(value), Set((bcInput.value, value)))
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
        val list = new Array[((Int, Int), Set[(Int, Int)])](v._2.size)
        var i = 0
        for (value <- v._2) {
          list(i) = (value, v._2.filterNot(_ == value))
          i += 1
        }
        list
      })
      .reduceByKey((v1, v2) => v1.intersect(v2))
      .flatMap(v => {
        val list = new Array[((Int, Int), (Int, Int))](v._2.size)
        var i = 0
        for (value <- v._2) {
          list(i) = (v._1, value)
          i += 1
        }
        list
      })
      .collect()
    for (bc <- fileIndices){
      bc.destroy()
    }
    for (dep <- result) {
      println(s"${inputs(dep._1._1).substring(10).stripSuffix(".csv")} -> ${
        inputs(dep._2._1).substring(10).stripSuffix(".csv")}: [${
        names(dep._1._1)(dep._1._2)}] c [${names(dep._2._1)(dep._2._2)}]")
    }
  }

}
