package de.mindlab.spark_schema_transformer.row_iterator.definitions

case class PathToken(
    strToken:        String,
    bBacktickNeeded: Boolean = false
)