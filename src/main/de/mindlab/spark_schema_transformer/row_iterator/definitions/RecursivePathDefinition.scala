package de.mindlab.spark_schema_transformer.row_iterator.definitions

import scala.collection.Seq


case class RecursivePathDefinition(
    oPathToken:                       PathToken                              = null,
    oTargetDataType:                  org.apache.spark.sql.types.DataType    = null, 
    seqChilds:                        Seq[ RecursivePathDefinition ]         = Seq(),
    oColumnTransformationDefinition:  ColumnTransformationDefinition         = null,
    bDataParentExistenceNeeded:       Boolean                                = true
)