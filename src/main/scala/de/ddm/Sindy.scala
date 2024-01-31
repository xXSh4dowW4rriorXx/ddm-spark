package de.ddm

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

    inputs.map(readData(_, spark))
      .map(_
        .flatMap(row => row.schema.fieldNames
          .map(v => (row.getString(row.fieldIndex(v)), Set(v))))
        .rdd
        .distinct())
      .reduce(_.union(_))
      .reduceByKey(_.union(_))
      .flatMap(v => v._2
        .map(value => (value, v._2.filterNot(_ == value))))
      .reduceByKey(_.intersect(_))
      .filter(_._2.nonEmpty)
      .sortBy(_._1)
      .collect()
      .foreach(dep => println(s"${dep._1} < ${dep._2.mkString("", ",", "")}"))
  }
}
