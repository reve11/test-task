package com.example.udwf

import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, Least, Literal}
import org.apache.spark.sql.types.{DataType, LongType}

case class SessionStartUDWF(eventTime: Expression) extends AggregateWindowFunction {

  self: Product =>

  private val currentStart: AttributeReference = AttributeReference("eventStart", LongType, nullable = true)()
  private val zero: Expression = Literal(Long.MaxValue, LongType)

  override val initialValues: Seq[Expression] = zero :: Nil
  override val updateExpressions: Seq[Expression] = Least(currentStart :: eventTime :: Nil) :: Nil
  override val evaluateExpression: Expression = aggBufferAttributes.head

  override def dataType: DataType = LongType

  override def aggBufferAttributes: Seq[AttributeReference] = currentStart :: Nil

  override def children: Seq[Expression] = Seq(eventTime)
}
