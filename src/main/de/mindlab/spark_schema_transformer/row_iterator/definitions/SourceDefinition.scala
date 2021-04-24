package de.mindlab.spark_schema_transformer.row_iterator.definitions


case class SourceDefinition( oPath:  PathDefinition     = null,
                             oConst: ConstantDefinition = null,
                         var mValue: Any                = null)