package com.example.udwf

import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, If, LessThanOrEqual, Literal, ScalaUDF, Subtract}
import org.apache.spark.sql.types.{DataType, LongType}

case class SessionIdUDWF(eventTime: Expression, sessionWindow: Expression) extends AggregateWindowFunction {
  self: Product =>

  private var currentId: Long = 0

  private val zero: Expression = Literal(0L)

  private val getNextSession = () => {
    currentId += 1
    currentId
  }

  private val currentSession: AttributeReference = AttributeReference("currentSession", LongType, nullable = true)()
  private val previousEventTime: AttributeReference = AttributeReference("previousEventTime", LongType, nullable = false)()

  private val assignSession = If(LessThanOrEqual(Subtract(eventTime, aggBufferAttributes(1)), sessionWindow),
    aggBufferAttributes.head,
    ScalaUDF(getNextSession, LongType, Nil))

  override def dataType: DataType = LongType

  override val initialValues: Seq[Expression] = zero :: zero :: Nil
  override val updateExpressions: Seq[Expression] =
    assignSession :: eventTime :: Nil
  override val evaluateExpression: Expression = aggBufferAttributes.head

  override def aggBufferAttributes: Seq[AttributeReference] = currentSession :: previousEventTime :: Nil

  override def children: Seq[Expression] = Seq(eventTime)
}
