package de.mindlab.spark_schema_transformer.row_iterator.definitions

import scala.collection.Seq


case class ColumnTransformationDefinition(
    oTargetPathDefinition:       PathDefinition,
    strFunctionName:             String,
    seqSourceDefinition:         Seq[SourceDefinition] = Seq(),
    bOverwriteTarget:            Boolean = false,
    bDataParentExistenceNeeded:  Boolean = true
)


//object ColumnTransformationDefinition{
//  
//  def loadFromLine( strLine: String ): ColumnTransformationDefinition = {
//    
//    println("ColumnTransformationDefinition.loadFromLine(): START " )
//    println("ColumnTransformationDefinition.loadFromLine(): Param strLine: \n" + strLine )
//    
//    
//    
//    println("ColumnTransformationDefinition.loadFromLine(): END " )
//    
//  }
//  
//  
//}