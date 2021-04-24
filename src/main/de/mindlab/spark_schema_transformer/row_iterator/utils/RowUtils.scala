package de.mindlab.spark_schema_transformer.row_iterator.utils


import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import de.mindlab.spark_schema_transformer.row_iterator.definitions._

object RowUtils {
  
    def getPathValueArray( 
      oPath:         PathDefinition,
      oSchema:       org.apache.spark.sql.types.ArrayType,
      seqElements:   Seq[Any]
  ): Any = {
      
      if ( null != seqElements )
      {
      
        val oDataType = oSchema.elementType
        
        seqElements.map( oElement => {
            
            
            if ( null != oElement )
            {
              oDataType match {
            
              case StructType(fields)          => {
                
                RowUtils.getPathValue(
                    oPath, 
                    oDataType.asInstanceOf[StructType], 
                    oElement.asInstanceOf[Row])
                
              }
              
              
              case ArrayType(elementType2, _)  => {
                
                RowUtils.getPathValueArray(
                    oPath, 
                    oDataType.asInstanceOf[ArrayType], 
                    oElement.asInstanceOf[Seq[Any]])
                
                
              }
              case _  => {
                null
              }
            
            
          }
        }
        else
          null
            
        }).toSeq
      }
      else
      {
        null
      }
      
  }

  
  def getPathValue( 
      oPath:    PathDefinition,
      oSchema:  org.apache.spark.sql.types.StructType,
      oRow:     org.apache.spark.sql.Row
  ): Any = 
  
  {
    val mRes = if ( null != oRow )
    {
      val seqPathTokens: Seq[PathToken] = oPath.seqPathTokens
      
      val oCurrentToken: PathToken  = seqPathTokens.head
      
      val nFieldIndex: Int = oSchema.fieldIndex( oCurrentToken.strToken )
      
      val oField = oSchema.fields(nFieldIndex)
      
      val oDataTypeOfField = oField.dataType
      
      val seqChildPathTokens: Seq[PathToken] = seqPathTokens.tail
      
      val bIsLeaf = seqChildPathTokens.size == 0
      
      if (bIsLeaf)
      {
        oRow.apply( nFieldIndex )
      }
      else
      {
        if ( oRow.isNullAt(nFieldIndex) )
        {
          null
        }
        else
        {
          oDataTypeOfField match {
            
            case StructType(fields)          => {
              
              RowUtils.getPathValue(
                  PathDefinition(seqChildPathTokens, oPath.strSeparator), 
                  oDataTypeOfField.asInstanceOf[StructType], 
                  oRow.getStruct( nFieldIndex ))
              
            }
            
            
            case ArrayType(elementType2, _)  => {
              
              RowUtils.getPathValueArray(
                  PathDefinition(seqChildPathTokens, oPath.strSeparator), 
                  oDataTypeOfField.asInstanceOf[ArrayType], 
                  oRow.getSeq( nFieldIndex ).asInstanceOf[Seq[Any]])
              
              
            }
            
            
            case _    => {
              
              null
              
              
            }
            
          }//-- oDataTypeOfField match
        }
      }
      
      
      
    }
    else
    {
      null
    }
    mRes
  }
  
  
  def fncGeneratorPathValue( 
      oPath:    PathDefinition,
      oSchema:  org.apache.spark.sql.types.StructType) = ( oRow:     org.apache.spark.sql.Row ) =>
  {
    
    RowUtils.getPathValue( 
      oPath,
      oSchema,
      oRow
    )
  }
  
  
  
}