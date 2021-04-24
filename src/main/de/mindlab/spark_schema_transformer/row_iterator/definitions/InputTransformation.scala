package de.mindlab.spark_schema_transformer.row_iterator.definitions

import org.apache.spark.sql.types.DataType

case class InputTransformation (depthlevel:Integer, path:String, dt:List[DataType])