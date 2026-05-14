package data.generator.intervals

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}


object DataSuites {

  def databaseUniformFlat(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .range(rowsCount / groupsCount)
      .toDF("index")
      .select(
        F.explode(F.lit(groups(groupsCount))).as("key"),
        F.col("index").as("from"),
        F.col("index").as("to")
      )

  def databaseUniformStacked(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .range(rowsCount / groupsCount)
      .toDF("index")
      .select(
        F.explode(F.lit(groups(groupsCount))).as("key"),
        (F.col("index") - F.lit(20)).as("from"),
        (F.col("index") + F.lit(20)).as("to")
      )
  }

  def databaseUniformHeavyStacked(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .range(rowsCount / groupsCount)
      .toDF("index")
      .select(
        F.explode(F.lit(groups(groupsCount))).as("key"),
        F.lit(0L).as("from"),
        (F.col("index") + F.lit(5)).as("to")
      )
  }

  def databaseSkewedFlat(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .range(rowsCount)
      .toDF("index")
      .withColumn("_rand", F.rand(42))
      .select(
        skewedGroupCol(groupsCount).as("key"),
        F.col("index").as("from"),
        F.col("index").as("to")
      )
  }

  def databaseSkewedStacked(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .range(rowsCount)
      .toDF("index")
      .withColumn("_rand", F.rand(42))
      .select(
        skewedGroupCol(groupsCount).as("key"),
        (F.col("index") - F.lit(20)).as("from"),
        (F.col("index") + F.lit(20)).as("to")
      )
  }

  def querySparse(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .range(rowsCount / groupsCount)
      .toDF("index")
      .select(
        F.explode(F.lit(groups(groupsCount))).as("key"),
        (F.lit(10) * F.col("index")).as("from"),
        (F.lit(10) * F.col("index")).as("to")
      )

  def queryDense(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .range(rowsCount / groupsCount)
      .toDF("index")
      .select(
        F.explode(F.lit(groups(groupsCount))).as("key"),
        (F.lit(10) * F.col("index")).as("from"),
        (F.lit(10) * F.col("index") + F.lit(9)).as("to")
      )

  def queryStacked(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .range(rowsCount / groupsCount)
      .toDF("index")
      .select(
        F.explode(F.lit(groups(groupsCount))).as("key"),
        (F.lit(10) * F.col("index") - F.lit(10)).as("from"),
        (F.lit(10) * F.col("index") + F.lit(19)).as("to")
      )

  def querySkewedDense(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .range(rowsCount)
      .toDF("index")
      .select(
        F.lit("CH-0").as("key"),
        (F.col("index") - F.lit(20)).as("from"),
        (F.col("index") + F.lit(20)).as("to")
      )
  }

  def queryDummy(rowsCount: Long, groupsCount: Int)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .range(rowsCount / (groupsCount * 4))
      .toDF("index")
      .select(
        F.explode(F.lit(groups(groupsCount * 4))).as("key"),
        (F.lit(10) * F.col("index")).as("from"),
        (F.lit(10) * F.col("index") + F.lit(9)).as("to")
      )


  private def groups(groupsCount: Int) =
    (0 until groupsCount).toArray.map(index => f"CH-$index")

  private def skewedGroupCol(groupsCount: Int) = F
    .when(F.col("_rand") > F.lit(0.5), "CH-0")
    .otherwise(F.concat(F.lit("CH-"),
      F.ceil(F.col("_rand") * F.lit(2) * F.lit(groupsCount - 1)).cast(DataTypes.IntegerType))
    )
}
