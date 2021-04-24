package de.mindlab.spark_schema_transformer.row_iterator.definitions

import scala.collection.mutable._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.DataType


case class AggregateTransformation(
    relatedTransformations:    PriorityQueue[InputTransformation]       
)





