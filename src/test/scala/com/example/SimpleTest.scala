package com.example

import java.net.URI
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
    val events = Main.parseEvents(defaultTestDataPath.toUri, spark)

    Assert.assertEquals(27, events.count())
  }

  @Test
  def should_correctly_determine_sesssions_by_window(): Unit = {
    val events = Main.parseEvents(defaultTestDataPath.toUri, spark)

    val byWindow = Main.getSessionedEventsByWindow(events, spark, DEFAULT_GAP)
    val sessionCategoryPairs = byWindow
      .select("category", "sessionId")
      .distinct()
      .count()

    assert(sessionCategoryPairs === 5)
  }

  @Test
  def sessions_with_udf_and_window_should_be_the_same(): Unit = {
    val events = Main.parseEvents(defaultTestDataPath.toUri, spark).persist()

    val sessionsByUDF = Main.getSessionedEventsByUDF(events, spark, DEFAULT_GAP)
      .groupBy("category", "sessionId")
      .count()
    val sessionsByWindow = Main.getSessionedEventsByWindow(events, spark, DEFAULT_GAP)
      .groupBy("category", "sessionId")
      .count()

    Assert.assertEquals(sessionsByUDF.count(), sessionsByWindow.count())
  }

  @Test
  def should_correctly_count_unique_user_visits_1(): Unit = {
    val testData: URI = copyData("test_data_unique_users.csv").toUri
    val parsed = Main.parseEvents(testData, spark)
    val sessionedEvents = Main.getSessionedEventsByWindow(parsed, spark, 300)
    val frame = Main.getUniqueUsersLengths(sessionedEvents, spark).persist()
    assert(frame.count() === 1)
    assert(frame.select("less_than_one").collect()(0).getLong(0) === 1)
    assert(frame.select("one_to_five").collect()(0).getLong(0) === 1)
    assert(frame.select("more_than_five").collect()(0).getLong(0) === 0)
  }

  @Test
  def should_correctly_count_unique_user_visits_2(): Unit = {
    val parsed = Main.parseEvents(defaultTestDataPath.toUri, spark)
    val sessionedEvents = Main.getSessionedEventsByWindow(parsed, spark, 300)
    val actual = Main.getUniqueUsersLengths(sessionedEvents, spark).persist()
    assert(actual.count() === 3)
    val books = actual.select("less_than_one", "one_to_five", "more_than_five")
      .where(actual("category") === "books").collect()(0)
    assert(books.getLong(0) === 0)
    assert(books.getLong(1) === 1)
    assert(books.getLong(2) === 1)
    val mobilePhones = actual.select("less_than_one", "one_to_five", "more_than_five")
      .where(actual("category") === "mobile phones").collect()(0)
    assert(mobilePhones.getLong(0) === 2)
    assert(mobilePhones.getLong(1) === 1)
    assert(mobilePhones.getLong(2) === 0)
    val notebooks = actual.select("less_than_one", "one_to_five", "more_than_five")
      .where(actual("category") === "notebooks").collect()(0)
    assert(notebooks.getLong(0) === 0)
    assert(notebooks.getLong(1) === 2)
    assert(notebooks.getLong(2) === 1)
  }
}

object SimpleTest {
  private var spark: SparkSession = _
  private var defaultTestDataPath: Path = _

  @BeforeClass
  def initAll(): Unit = {
    defaultTestDataPath = copyData("test_data.csv")

    spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()
  }

  private def copyData(fileName: String): Path = {
    val testDataStream = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val result = Files.createTempFile("spark_test", ".csv")
    Files.copy(testDataStream, result, StandardCopyOption.REPLACE_EXISTING)
    result
  }

  @AfterClass
  def destroyAll(): Unit = {
    spark.close()
  }
}
