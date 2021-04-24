package de.mindlab.spark_schema_transformer.row_iterator.definitions

import org.apache.spark.sql.Row
import scala.collection.Seq


case class PathDefinition(
    seqPathTokens: Seq[PathToken],
    strSeparator: String = "/",
    fncValueFetching: org.apache.spark.sql.Row => Any = null
)