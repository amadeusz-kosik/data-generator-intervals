package data.generator.intervals

import data.generator.intervals.test.WithDataFrameAssertions
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec


class DataSuitesTestSuite extends AnyFunSpec with WithDataFrameAssertions {
  private val TestDataGroupsCount = 2
  private val TestDataSize = 10

  describe("DataSuites.databaseUniformFlat") {
    it("should generate set of consecutive point intervals") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = DataSuites.databaseUniformFlat(TestDataSize, TestDataGroupsCount)
      val expectedData = List(
        DataSuiteTestRow("CH-0", 0, 0),
        DataSuiteTestRow("CH-0", 1, 1),
        DataSuiteTestRow("CH-0", 2, 2),
        DataSuiteTestRow("CH-0", 3, 3),
        DataSuiteTestRow("CH-0", 4, 4),
        DataSuiteTestRow("CH-1", 0, 0),
        DataSuiteTestRow("CH-1", 1, 1),
        DataSuiteTestRow("CH-1", 2, 2),
        DataSuiteTestRow("CH-1", 3, 3),
        DataSuiteTestRow("CH-1", 4, 4)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.databaseUniformStacked") {
    it("should generate set of consecutive wide intervals, each overlapping some of its neighbours") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = DataSuites.databaseUniformStacked(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", -20, 20),
        DataSuiteTestRow("CH-0", -19, 21),
        DataSuiteTestRow("CH-0", -18, 22),
        DataSuiteTestRow("CH-0", -17, 23),
        DataSuiteTestRow("CH-0", -16, 24),
        DataSuiteTestRow("CH-1", -20, 20),
        DataSuiteTestRow("CH-1", -19, 21),
        DataSuiteTestRow("CH-1", -18, 22),
        DataSuiteTestRow("CH-1", -17, 23),
        DataSuiteTestRow("CH-1", -16, 24)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.databaseUniformHeavyStacked") {
    it("should generate set of consecutive intervals, all starting to 0 and lasting incrementally further") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = DataSuites.databaseUniformHeavyStacked(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", 0, 5),
        DataSuiteTestRow("CH-0", 0, 6),
        DataSuiteTestRow("CH-0", 0, 7),
        DataSuiteTestRow("CH-0", 0, 8),
        DataSuiteTestRow("CH-0", 0, 9),
        DataSuiteTestRow("CH-1", 0, 5),
        DataSuiteTestRow("CH-1", 0, 6),
        DataSuiteTestRow("CH-1", 0, 7),
        DataSuiteTestRow("CH-1", 0, 8),
        DataSuiteTestRow("CH-1", 0, 9)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.databaseSkewedFlat") {
    it("should generate set of consecutive intervals, but the first group should be bigger than any other") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = DataSuites.databaseSkewedFlat(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", 0, 0),
        DataSuiteTestRow("CH-0", 1, 1),
        DataSuiteTestRow("CH-0", 2, 2),
        DataSuiteTestRow("CH-1", 3, 3),
        DataSuiteTestRow("CH-1", 4, 4),
        DataSuiteTestRow("CH-0", 5, 5),
        DataSuiteTestRow("CH-0", 6, 6),
        DataSuiteTestRow("CH-0", 7, 7),
        DataSuiteTestRow("CH-1", 8, 8),
        DataSuiteTestRow("CH-0", 9, 9)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.databaseSkewedStacked") {
    it("should generate set of increasingly overlapping intervals, but the first group should be bigger than any other") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = DataSuites.databaseSkewedStacked(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", -20, 20),
        DataSuiteTestRow("CH-0", -19, 21),
        DataSuiteTestRow("CH-0", -18, 22),
        DataSuiteTestRow("CH-1", -17, 23),
        DataSuiteTestRow("CH-1", -16, 24),
        DataSuiteTestRow("CH-0", -15, 25),
        DataSuiteTestRow("CH-0", -14, 26),
        DataSuiteTestRow("CH-0", -13, 27),
        DataSuiteTestRow("CH-1", -12, 28),
        DataSuiteTestRow("CH-0", -11, 29)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.querySparse") {
    it("should generate set of increasingly non-overlapping intervals with some padding between them") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = DataSuites.querySparse(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0",  0,  4),
        DataSuiteTestRow("CH-0", 10, 14),
        DataSuiteTestRow("CH-0", 20, 24),
        DataSuiteTestRow("CH-0", 30, 34),
        DataSuiteTestRow("CH-0", 40, 44),
        DataSuiteTestRow("CH-1",  0,  4),
        DataSuiteTestRow("CH-1", 10, 14),
        DataSuiteTestRow("CH-1", 20, 24),
        DataSuiteTestRow("CH-1", 30, 34),
        DataSuiteTestRow("CH-1", 40, 44)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.queryDense") {
    it("should generate set of increasingly non-overlapping intervals without any padding between them") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = DataSuites.queryDense(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0",  0,  9),
        DataSuiteTestRow("CH-0", 10, 19),
        DataSuiteTestRow("CH-0", 20, 29),
        DataSuiteTestRow("CH-0", 30, 39),
        DataSuiteTestRow("CH-0", 40, 49),
        DataSuiteTestRow("CH-1",  0,  9),
        DataSuiteTestRow("CH-1", 10, 19),
        DataSuiteTestRow("CH-1", 20, 29),
        DataSuiteTestRow("CH-1", 30, 39),
        DataSuiteTestRow("CH-1", 40, 49)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }

  describe("DataSuites.queryStacked") {
    it("should generate set of increasingly overlapping intervals") {
      implicit val _sparkSession: SparkSession = sparkSession
      import _sparkSession.implicits._

      val generatedDataSample = DataSuites.queryStacked(TestDataSize, TestDataGroupsCount)

      val expectedData = List(
        DataSuiteTestRow("CH-0", -10, 19),
        DataSuiteTestRow("CH-0",   0, 29),
        DataSuiteTestRow("CH-0",  10, 39),
        DataSuiteTestRow("CH-0",  20, 49),
        DataSuiteTestRow("CH-0",  30, 59),
        DataSuiteTestRow("CH-1", -10, 19),
        DataSuiteTestRow("CH-1",   0, 29),
        DataSuiteTestRow("CH-1",  10, 39),
        DataSuiteTestRow("CH-1",  20, 49),
        DataSuiteTestRow("CH-1",  30, 59)
      ).toDF()

      assertDataFramesEqual(expectedData, generatedDataSample)
    }
  }
}

case class DataSuiteTestRow(key: String, from: Long, to: Long)