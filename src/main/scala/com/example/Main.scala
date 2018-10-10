package com.example

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

object Main {

  private val FIVE_MINUTES: Long = Duration.of(5, ChronoUnit.MINUTES).getSeconds
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    val spark = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    implicit val eventEncoder: Encoder[Event] = Encoders.product[Event]
    import org.apache.spark.sql.expressions.Window

    val categoryWindow = Window.partitionBy('category).orderBy('eventTime)
    val globalCategoryWindow = Window.orderBy('category, 'eventTime)
    val sessionWindow = Window.partitionBy('sessionId).orderBy('eventTime)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val lastEvent = lag('eventTime, 1).over(categoryWindow).as("lastEvent")
    val sessionId = sum($"isNewSession").over(globalCategoryWindow).as("sessionId")
    val sessionStart = min($"eventTime").over(sessionWindow).as("sessionStart")
    val sessionEnd = max($"eventTime").over(sessionWindow).as("sessionEnd")

    val sessionBoundaries = $"eventTime".minus($"lastEvent").gt(FIVE_MINUTES).or($"lastEvent".isNull)

    val userCategoryWindow = Window.partitionBy('category, 'userId).orderBy('eventTime)
    val userTimeSpent = ($"eventTime" - coalesce(lag('eventTime, 1).over(userCategoryWindow), $"eventTime"))
      .as("spentTime")
    val ltOne = when($"spentTime" < 60, 1).otherwise(0)
      .as("less_than_one")
    val gtFive = when($"spentTime" > 300, 1).otherwise(0)
      .as("more_than_five")
    val oneToFive = when($"spentTime" >= 60 and $"spentTime" <= 300, 1).otherwise(0)
      .as("from_one_to_five")

    val parsed = spark.read
      .option("header", "true")
      .csv("/tmp/test_data.csv")
      .map(parseEvent)
      .persist()

    val groupedDf = parsed
      .select($"*", lastEvent)
      .select($"*", when(sessionBoundaries, 1).otherwise(0).as("isNewSession"))
      .select($"*", sessionId)
      .select($"category", $"product", $"userId", $"eventType", $"eventTime", $"sessionId", sessionStart, sessionEnd)

    groupedDf.createOrReplaceTempView("sessioned_events")

    spark.sql("select category, percentile_approx(sessionEnd - sessionStart, 0.5) as median_session_time from sessioned_events group by category")
      .show()

    groupedDf
      .select($"*", userTimeSpent)
      .groupBy($"category", $"userId")
      .agg(sum($"spentTime").as("spentTime"))
      .select($"*", ltOne, oneToFive, gtFive)
      .show()

    val lastEventUserProduct = lag('eventTime, 1).over(Window.partitionBy('userId).orderBy('eventTime))
      .as("lastUserEvent")
    val tmpWindow = Window.partitionBy('userId).orderBy('userId, 'eventTime)
    val isNewUserSession = when(lag('product, 1).over(tmpWindow).notEqual($"product")
      .or(lastEventUserProduct.isNull), 1)
      .otherwise(0).as("isNewSession")


    val sessionTimeWindow = Window.partitionBy('sessionId).orderBy('eventTime).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    parsed
      .select($"*", lastEventUserProduct, isNewUserSession)
      .select($"*", sum($"isNewSession").over(Window.orderBy('userId, 'eventTime)).as("sessionId"))
      .select($"*", first($"eventTime").over(sessionTimeWindow).as("sessionStart"))
      .select($"*", last($"eventTime").over(sessionTimeWindow).as("sessionEnd"))
      .select($"sessionId", $"sessionEnd".minus($"sessionStart").as("sessionLength"), $"category", $"product")
      .distinct()
      //      .select(rank().over())
      .show(100)

    spark.close()


  }

  case class Event(category: String, product: String, userId: String, eventTime: Long, eventType: String)

  private val parseEvent: Row => Event = row => Event(row.getString(0), row.getString(1), row.getString(2),
    LocalDateTime.parse(row.getString(3), formatter).toEpochSecond(ZoneOffset.UTC), row.getString(4))

}
