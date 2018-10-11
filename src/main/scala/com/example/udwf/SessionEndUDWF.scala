package com.example.udwf

import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, Greatest, Literal}
import org.apache.spark.sql.types.{DataType, LongType}

case class SessionEndUDWF(eventTime: Expression) extends AggregateWindowFunction {

  self: Product =>

  private val currentEnd: AttributeReference = AttributeReference("eventStart", LongType, nullable = true)()
  private val zero: Expression = Literal(Long.MinValue, LongType)

  override val initialValues: Seq[Expression] = zero :: Nil
  override val updateExpressions: Seq[Expression] = Greatest(eventTime :: currentEnd :: Nil) :: Nil
  override val evaluateExpression: Expression = aggBufferAttributes.head

  override def dataType: DataType = LongType

  override def aggBufferAttributes: Seq[AttributeReference] = currentEnd :: Nil

  override def children: Seq[Expression] = eventTime :: Nil
}
