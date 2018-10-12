package com.example

import java.net.URI
import java.time._
import java.time.format.DateTimeFormatter

import com.example.udwf.SessionIdUDWF
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.Try

object Main {

  private val FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println(
        """Application requires two arguments: session gap in seconds and file URI.
          | Example: test.jar 300 file:///tmp/test.csv""".stripMargin)
      System.exit(2)
    }

    val gap = Try(args(0).toInt)
    if (gap.isFailure) {
      System.err.println("""First argument should be an integer number""")
      System.exit(2)
    }
    val fileUri = Try(new URI(args(1)))
    if (fileUri.isFailure) {
      System.err.println("""Second argument should a valid URI""")
      System.exit(2)
    }

    val conf: SparkConf = new SparkConf()
    val spark = SparkSession.builder()
      .config(conf)
      .appName("test")
      .getOrCreate()

    import spark.implicits._

    val parsed = parseEvents(fileUri.get, spark)

    val sessionedEvents = getSessionedEventsByWindow(parsed, spark, gap.get)

    //Task#2 1
    sessionedEvents.createOrReplaceTempView("sessioned_events")
    spark.sql(
      """select category, percentile_approx(sessionEnd - sessionStart, 0.5) as median_session_time
        | from sessioned_events
        |  group by category""".stripMargin)
      .show()

    //Task#2 2
    val userCategoryWindow = Window.partitionBy('category, 'userId, 'sessionId).orderBy('eventTime)
    val userTimeSpent = ($"eventTime" - coalesce(lag('eventTime, 1).over(userCategoryWindow), $"eventTime"))
      .as("spentTime")
    val categorySessionWindow = Window.partitionBy('category, 'sessionId).orderBy('category)
    val ltOne = sum(when($"spentTime" < 60, 1).otherwise(0))
      .as("less_than_one")
    val gtFive = sum(when($"spentTime" > 300, 1).otherwise(0))
      .as("more_than_five")
    val oneToFive = sum(when($"spentTime" >= 60 and $"spentTime" <= 300, 1).otherwise(0))
      .as("one_to_five")


    sessionedEvents
      .select($"*", userTimeSpent)
      .groupBy($"category", $"userId", $"sessionId")
      .agg(sum($"spentTime").as("spentTime"))
      .groupBy($"category")
      .agg(ltOne, oneToFive, gtFive)
      .show()

    //Task#2 3
    val userSessionedDF = getUserSessionedEvents(parsed, spark)

    userSessionedDF
      .groupBy($"category", $"product")
      .agg(sum($"sessionLength").as("totalLength"))
      .select($"*", rank().over(Window.partitionBy('category).orderBy('totalLength desc)).as("rank"))
      .where($"rank" <= 10)
      .show()


    spark.close()


  }

  /**
    * Parses events from file by specified URI. Unparsable events are discarded
    *
    * @param fileUri file URI
    * @param spark   spark session
    * @return Dataset of events
    */
  private[example] def parseEvents(fileUri: URI, spark: SparkSession): Dataset[Event] = {
    implicit val eventEncoder: Encoder[Event] = Encoders.product[Event]
    implicit val tryEncoder: Encoder[Try[Event]] = Encoders.kryo[Try[Event]]

    spark.read
      .option("header", "true")
      .csv(fileUri.toString)
      .map(parseEvent)
      .filter(_.isSuccess)
      .map(_.get)
      .persist()
  }

  /**
    * Enrich events with session id and session start/end with the help of custom UDWF. Definition of a session:
    * it contains consecutive events that belong to a single category and are not more than gap seconds away from
    * each other.
    *
    * @param dataset events to enrich
    * @param spark   spark session
    * @param gap     time in seconds to define session boundaries
    * @return enriched dataframe
    */
  private[example] def getSessionedEventsByUDF(dataset: Dataset[Event], spark: SparkSession, gap: Long): DataFrame = {
    import spark.implicits._

    val categoryWindow = Window.partitionBy('category).orderBy('eventTime)
    val sessionId = new Column(SessionIdUDWF($"eventTime".expr, lit(gap).expr)).over(categoryWindow)
    val categorySessionWindow = Window.partitionBy('category, 'sessionId).orderBy('eventTime)
    val sessionStart = min($"eventTime").over(categorySessionWindow).as("sessionStart")
    val sessionEnd = max($"eventTime").over(categorySessionWindow.orderBy('eventTime desc)).as("sessionEnd")
    dataset
      .withColumn("sessionId", sessionId)
      .withColumn("sessionStart", sessionStart)
      .withColumn("sessionEnd", sessionEnd)
  }

  /**
    * Enrich incoming data with sessions using window functions. Definition of a session: it contains consecutive events
    * that belong to a single category and are not more than gap seconds away from each other.
    *
    * @param dataset data to enrich
    * @param spark   spark session
    * @param gap     time in seconds to define session boundaries
    * @return enriched data with sessions
    */
  private[example] def getSessionedEventsByWindow(dataset: Dataset[Event], spark: SparkSession, gap: Long): DataFrame = {
    import spark.implicits._

    val categoryWindow = Window.partitionBy('category).orderBy('eventTime)
    val sessionWindow = Window.partitionBy('category, 'sessionId).orderBy('eventTime)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val lastEvent = lag('eventTime, 1).over(categoryWindow).as("lastEvent")
    val sessionId = sum($"isNewSession").over(categoryWindow).as("sessionId")
    val sessionStart = min($"eventTime").over(sessionWindow).as("sessionStart")
    val sessionEnd = max($"eventTime").over(sessionWindow.orderBy('eventTime desc)).as("sessionEnd")
    val sessionBoundaries = $"eventTime".minus($"lastEvent").gt(gap)
      .or($"lastEvent".isNull)

    dataset
      .withColumn("lastEvent", lastEvent)
      .withColumn("isNewSession", when(sessionBoundaries, 1).otherwise(0))
      .withColumn("sessionId", sessionId)
      .withColumn("sessionStart", sessionStart)
      .withColumn("sessionEnd", sessionEnd)
      .drop("lastEvent", "isNewSession")
  }

  /**
    * Enrich events with user sessions
    *
    * @param dataset events to enrich
    * @param spark   spark session
    * @return
    */
  private[example] def getUserSessionedEvents(dataset: Dataset[Event], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val userWindow = Window.partitionBy('userId).orderBy('eventTime)
    val lastEvent = lag('eventTime, 1).over(userWindow).as("lastEvent")
    val isNewSession = when(lag('product, 1).over(userWindow).notEqual($"product")
      .or(lastEvent.isNull), 1)
      .otherwise(0).as("isNewSession")

    val sessionTimeWindow = Window.partitionBy('sessionId).orderBy('eventTime)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val sessionStart = first($"eventTime").over(sessionTimeWindow).as("sessionStart")
    val sessionEnd = last($"eventTime").over(sessionTimeWindow).as("sessionEnd")

    dataset
      .withColumn("lastEvent", lastEvent)
      .withColumn("isNewSession", isNewSession)
      .withColumn("sessionId", sum($"isNewSession").over(Window.orderBy('userId, 'eventTime)))
      .withColumn("sessionLength", sessionEnd.minus(sessionStart))
      .select($"sessionId", $"sessionLength", $"category", $"product", $"userId")
      .distinct()
  }

  case class Event(category: String, product: String, userId: String, eventTime: Long, eventType: String)

  private[example] val parseEvent: Row => Try[Event] = row => Try(Event(row.getString(0), row.getString(1), row.getString(2),
    LocalDateTime.parse(row.getString(3), FORMATTER).toEpochSecond(ZoneOffset.UTC), row.getString(4)))

}
