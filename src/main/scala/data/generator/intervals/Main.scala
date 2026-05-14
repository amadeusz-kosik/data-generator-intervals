package data.generator.intervals

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Main extends App {
  private val DatabaseRowsCount   = 10000000L
  private val DatabaseGroupsCount = 10
  private val TargetPartitionsCount = 8

  private implicit val sparkSession: SparkSession = SparkSession.builder()
    .appName("Data generator for intervals data")
    .getOrCreate()

  private val outputPath = "/mnt/data/synthetic"

  Map[String, (Long, Int) => DataFrame](
    "databaseUniformFlat"         -> DataSuites.databaseUniformFlat,
    "databaseUniformStacked"      -> DataSuites.databaseUniformStacked,
    "databaseUniformHeavyStacked" -> DataSuites.databaseUniformHeavyStacked,
    "databaseSkewedFlat"          -> DataSuites.databaseSkewedFlat,
    "databaseSkewedStacked"       -> DataSuites.databaseSkewedStacked,
    "querySparse"                 -> DataSuites.querySparse,
    "queryDense"                  -> DataSuites.queryDense,
    "querySkewedDense"            -> DataSuites.querySkewedDense,
    "queryDummy"                  -> DataSuites.queryDummy
  ) foreach { case (name, callback) =>
    callback(DatabaseRowsCount, DatabaseGroupsCount)
      .repartition(TargetPartitionsCount)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(f"$outputPath/$name.parquet")
  }
}
