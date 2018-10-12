package com.example

import java.nio.file.{Files, Path, StandardCopyOption}
import java.time.Duration
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.SparkSession
import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.scalatest.junit.AssertionsForJUnit

class SimpleTest extends AssertionsForJUnit {

  private val DEFAULT_GAP = Duration.of(5, ChronoUnit.MINUTES).getSeconds

  import SimpleTest._

  @Test
  def should_correctly_parse_csv(): Unit = {
    val events = Main.parseEvents(filePath.toUri, spark)

    Assert.assertEquals(27, events.count())
  }

  @Test
  def should_correctly_determine_sesssions_by_window(): Unit = {
    val events = Main.parseEvents(filePath.toUri, spark)

    val byWindow = Main.getSessionedEventsByWindow(events, spark, DEFAULT_GAP)
    val sessionCategoryPairs = byWindow
      .select("category", "sessionId")
      .distinct()
      .count()

    assert(sessionCategoryPairs === 5)
  }

  @Test
  def sessions_with_udf_and_window_should_be_the_same(): Unit = {
    val events = Main.parseEvents(filePath.toUri, spark).persist()

    val sessionsByUDF = Main.getSessionedEventsByUDF(events, spark, DEFAULT_GAP)
      .groupBy("category", "sessionId")
      .count()
    val sessionsByWindow = Main.getSessionedEventsByWindow(events, spark, DEFAULT_GAP)
      .groupBy("category", "sessionId")
      .count()

    Assert.assertEquals(sessionsByUDF.count(), sessionsByWindow.count())
  }
}

object SimpleTest {
  private var spark: SparkSession = _
  private var filePath: Path = _

  @BeforeClass
  def initAll(): Unit = {
    val testDataStream = this.getClass.getClassLoader.getResourceAsStream("test_data.csv")
    filePath = Files.createTempFile("spark_test", ".csv")
    Files.copy(testDataStream, filePath, StandardCopyOption.REPLACE_EXISTING)

    spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()
  }

  @AfterClass
  def destroyAll(): Unit = {
    spark.close()
  }
}
