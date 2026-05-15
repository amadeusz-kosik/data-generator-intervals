package data.generator.intervals

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Main extends App {
  private val DatabaseGroupsCount = 10
  private val TargetPartitionsCount = 8

  private val Datasets = Map[String, (Long, Int) => DataFrame](
    "databaseUniformFlat"         -> DataSuites.databaseUniformFlat,
    "databaseUniformStacked"      -> DataSuites.databaseUniformStacked,
    "databaseUniformHeavyStacked" -> DataSuites.databaseUniformHeavyStacked,
    "databaseSkewedFlat"          -> DataSuites.databaseSkewedFlat,
    "databaseSkewedStacked"       -> DataSuites.databaseSkewedStacked,
    "querySparse"                 -> DataSuites.querySparse,
    "queryDense"                  -> DataSuites.queryDense,
    "querySkewedDense"            -> DataSuites.querySkewedDense,
    "queryDummy"                  -> DataSuites.queryDummy
  )

  private val DatabaseSizes = Array(
      100000L,
     1000000L,
    10000000L
  )

  private implicit val sparkSession: SparkSession = SparkSession.builder()
    .appName("Data generator for intervals data")
    .getOrCreate()

  private val outputPath = "/mnt/data/synthetic"

  Datasets.foreach { case (name, callback) =>
    DatabaseSizes.foreach { (databaseRowsCount) =>
      callback(databaseRowsCount, DatabaseGroupsCount)
        .repartition(TargetPartitionsCount)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(f"$outputPath/$databaseRowsCount/$name.parquet")
    }
  }
}
