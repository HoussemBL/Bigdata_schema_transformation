package de.mindlab.spark_schema_transformer.row_iterator

import de.mindlab.spark_schema_transformer.row_iterator.definitions._
import org.apache.spark.sql.Row

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType



import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.spark.sql.AccessShowString.showString

case class SchemaDiff( 
    bErrorOcurred:                 Boolean, 
    seqDissapearedOriginalDfPaths: Seq[StructType],
    seqNewDfPaths:                 Seq[StructType],
    seqUnchangedDfPaths:           Seq[StructType]
    )

    
    
case class ValueDiff(
    bPrimaryKeyIsOkOriginal:      Boolean,
    bPrimaryKeyIsOkTransformed:   Boolean,
    bUnchangedColumnsEqual:       Boolean,
    seqDifferentUnchangedColumns: Seq[StructType]
    
    
    
)
    

object Comparator {
  
  
  def compare( 
      seqPrimaryKeyColumnNames: Seq[String],
      oSchemaDiff:              SchemaDiff,
      oOriginalDf:              org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ], 
      oTransformedDf:           org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      nRows:                    Int = 10)
  : ( ValueDiff, String, String ) =
  {
    println( "de.mindlab.spark_schema_transformer.row_iterator.compare(): " + "START" )
    
    val bPrimaryKeyIsOkOriginal:    Boolean = Comparator.checkPrimaryKey( seqPrimaryKeyColumnNames, oOriginalDf    )
    val bPrimaryKeyIsOkTransformed: Boolean = Comparator.checkPrimaryKey( seqPrimaryKeyColumnNames, oTransformedDf )
    
    
    val oRes = if ( bPrimaryKeyIsOkOriginal && bPrimaryKeyIsOkTransformed )
    {
      
      // Check unchanged columns
      
      val seqDifferentUnchangedDfPaths: Seq[StructType] = Comparator.compareUnchangedDfPaths( 
                                                                    seqPrimaryKeyColumnNames,
                                                                    oSchemaDiff.seqUnchangedDfPaths,
                                                                    oOriginalDf, 
                                                                    oTransformedDf )
      
      
      
      
      val strNewPathValues: String = Comparator.getNewTransformedDfPathValues( 
      seqPrimaryKeyColumnNames,
      oSchemaDiff.seqNewDfPaths,
      oTransformedDf,
      nRows
      )
  
      val strDissapearedPathValues: String = Comparator.getDissapearedOriginalDfPathValues( 
      seqPrimaryKeyColumnNames,
      oSchemaDiff.seqDissapearedOriginalDfPaths,
      oOriginalDf,
      nRows
      )
      
      
      ( ValueDiff( bPrimaryKeyIsOkOriginal, bPrimaryKeyIsOkTransformed, seqDifferentUnchangedDfPaths.length == 0, seqDifferentUnchangedDfPaths ), strDissapearedPathValues, strNewPathValues)
    }
    else
    {
       ( ValueDiff( bPrimaryKeyIsOkOriginal, bPrimaryKeyIsOkTransformed, false, null ), "", "" )
    }
    
    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.compare(): " + "END" )
    
    
    
    oRes
  }
  
  
  // seqNewDfPaths:                 Seq[StructType],
  
  def getNewTransformedDfPathValues( 
      seqPrimaryKeyColumnNames:       Seq[String],
      seqNewDfPaths:                  Seq[StructType],
      oTransformedDf:                 org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      nRows:                          Int = 10
      )
  : String = {
    
    
    
    val oRes: Seq[String] = seqNewDfPaths.map( oColumnPath => {
      
      val oCurrentTransformedPathDfTuple       = Comparator.getDfStructType( oTransformedDf,  seqPrimaryKeyColumnNames, oColumnPath)
      val seqCurrentTransformedPrimaryKey      = oCurrentTransformedPathDfTuple._1
      val strCurrentTransformedValueColumnName = oCurrentTransformedPathDfTuple._2
      val oCurrentTransformedPathDf            = oCurrentTransformedPathDfTuple._3
      
      
      strCurrentTransformedValueColumnName + ":\n" + showString(oCurrentTransformedPathDf, nRows, 100, false) + "\n"
    })
    
    oRes.toString
  }//-- def getDissapearedOriginalDfPathValues(...)
  
  
  
  def getDissapearedOriginalDfPathValues( 
      seqPrimaryKeyColumnNames:       Seq[String],
      seqDissapearedOriginalDfPaths:  Seq[StructType],
      oOriginalDf:                    org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      nRows:                          Int = 10
      )
  : String = {
    
    
    
    val oRes: Seq[String] = seqDissapearedOriginalDfPaths.map( oColumnPath => {
      
      val oCurrentOriginalPathDfTuple       = Comparator.getDfStructType( oOriginalDf,  seqPrimaryKeyColumnNames, oColumnPath)
      val seqCurrentOriginalPrimaryKey      = oCurrentOriginalPathDfTuple._1
      val strCurrentOriginalValueColumnName = oCurrentOriginalPathDfTuple._2
      val oCurrentOriginalPathDf            = oCurrentOriginalPathDfTuple._3
      
      
      strCurrentOriginalValueColumnName + ":\n" + showString(oCurrentOriginalPathDf, nRows, 100, false) + "\n"
    })
    
    oRes.toString
  }//-- def getDissapearedOriginalDfPathValues(...)
  
  
  
  
  def compareUnchangedDfPaths( 
      seqPrimaryKeyColumnNames: Seq[String],
      seqUnchangedDfPaths:      Seq[StructType],
      oOriginalDf:              org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ], 
      oTransformedDf:           org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] )
  : Seq[StructType] = {
    
    var seqResults: Seq[( StructType, Boolean, Boolean, Boolean, org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] )] = Seq()
    val nNumberOfPaths = seqUnchangedDfPaths.length
    var nPosPath = nNumberOfPaths
    
    nPosPath = 1
    
    seqUnchangedDfPaths.par.foreach( oColumnPath => {
      
      val oCurrentOriginalPathDfTuple       = Comparator.getDfStructType( oOriginalDf,  seqPrimaryKeyColumnNames, oColumnPath)
      val seqCurrentOriginalPrimaryKey      = oCurrentOriginalPathDfTuple._1
      val strCurrentOriginalValueColumnName = oCurrentOriginalPathDfTuple._2
      val oCurrentOriginalPathDf            = oCurrentOriginalPathDfTuple._3
      
      val oCurrentTransformedPathDfTuple       = Comparator.getDfStructType( oTransformedDf,  seqPrimaryKeyColumnNames, oColumnPath)
      val seqCurrentTransformedPrimaryKey      = oCurrentTransformedPathDfTuple._1
      val strCurrentTransformedValueColumnName = oCurrentTransformedPathDfTuple._2
      val oCurrentTransformedPathDf            = oCurrentTransformedPathDfTuple._3
      
      val bPrimaryKeysEqual:      Boolean = ( 0 == seqCurrentOriginalPrimaryKey.diff( seqCurrentTransformedPrimaryKey ).length ) && ( 0 == seqCurrentTransformedPrimaryKey.diff( seqCurrentOriginalPrimaryKey).length )
      
      val bValueColumnNamesEqual: Boolean = strCurrentOriginalValueColumnName.equals( strCurrentTransformedValueColumnName )
      
      
      
      
      
      val oLocalRes = if ( bPrimaryKeysEqual && bValueColumnNamesEqual )
      {
        if ( !seqPrimaryKeyColumnNames.contains( strCurrentOriginalValueColumnName ) )
        {
          val strOriginalValueColumnName    = "original."    + strCurrentOriginalValueColumnName
          val strTransformedValueColumnName = "transformed." + strCurrentTransformedValueColumnName
          
          val oJoinedDf = oCurrentOriginalPathDf.withColumnRenamed(strCurrentOriginalValueColumnName, strOriginalValueColumnName).join(oCurrentTransformedPathDf.withColumnRenamed(strCurrentTransformedValueColumnName, strTransformedValueColumnName) , seqCurrentOriginalPrimaryKey, "full_outer")
          
          val oColumnOriginal    = oJoinedDf.col( "`" + strOriginalValueColumnName    + "`" )
          val oColumnTransformed = oJoinedDf.col( "`" + strTransformedValueColumnName + "`" )
          
          val differentValuesDf = oJoinedDf.filter( oColumnOriginal !== oColumnTransformed )
          
          
          oOriginalDf.sparkSession.sparkContext.setJobGroup("Compare unchanged paths", "Count different values df" )
          val bColumnPathEqual = ( 0 == differentValuesDf.count ) 
          
          
          
          ( oColumnPath, bPrimaryKeysEqual, bValueColumnNamesEqual, bColumnPathEqual, oJoinedDf )
        }
        else
        {
          ( oColumnPath, bPrimaryKeysEqual, bValueColumnNamesEqual, true, null )
        }
        
      }
      else
      {
        ( oColumnPath, bPrimaryKeysEqual, bValueColumnNamesEqual, false, null )
      }
      
      
      seqResults.synchronized(
          {
            println( "de.mindlab.spark_schema_transformer.row_iterator.Comparator.compareUnchangedDfPaths(): Path " + nPosPath + " of " + nNumberOfPaths + ". Var bPrimaryKeysEqual: " + oLocalRes._2 + ", var bValueColumnNamesEqual: " + oLocalRes._3 + ", val bColumnPathEqual: " + oLocalRes._4 + ".")
//            if (null != oLocalRes._5) oLocalRes._5.show(false)
            seqResults = seqResults :+ oLocalRes
            nPosPath = nPosPath +1
          }
      )
      
    })
    
    val seqDifferentResults: Seq[StructType] = seqResults.filter( x => { !( x._2 && x._3 && x._4 ) }).map( x => { x._1 } ) //: Seq[( StructType, Boolean, Boolean, Boolean )].
    
    
    seqDifferentResults
  }//-- def compareUnchangedDfPaths() ---
  
  
  
  
  def getDfArrayType( df: org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],  seqPrimaryKeyColumnNames: Seq[String], oColumnPath: ArrayType, strColumnNamePrefix: String = "", nLevel: Int =0 ):
  ( Seq[String], String, org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] ) = {
    
    val seqKeyColumns = seqPrimaryKeyColumnNames.map( x => { df.col("`" + x + "`") })
    
    val strNextPrefix = strColumnNamePrefix
    
    val oDataType: DataType = oColumnPath.elementType
    
    val oColumn = df.col("`" + strColumnNamePrefix + "`")
    
    val oColumnExploded = posexplode_outer(oColumn)
    
    val seqAllColumns = seqKeyColumns :+ oColumnExploded
    
    val strAdditionalKey = "array_id_level_" + nLevel
    
    val newDf = df.select( seqAllColumns: _* )
                                  .withColumnRenamed("pos", strAdditionalKey)
                                  .withColumnRenamed("col", strNextPrefix)
                                  
                                  
                                  
    ;
    
    val seqNewPrimaryKeyColumnNames = seqPrimaryKeyColumnNames :+ strAdditionalKey
    
    oDataType   match {
        case StructType(fields)         => {
          
          val oResDf = Comparator.getDfStructType( newDf, seqNewPrimaryKeyColumnNames, oDataType.asInstanceOf[StructType], strNextPrefix, nLevel+1 )
          
          
          oResDf
        }
        
        case ArrayType(elementType, _)  => {
          
          val oResDf = Comparator.getDfArrayType( newDf, seqNewPrimaryKeyColumnNames, oDataType.asInstanceOf[ArrayType], strNextPrefix, nLevel+1 )
          
          oResDf
        }
        
        case _                          => {
          
          (seqNewPrimaryKeyColumnNames, strNextPrefix, newDf)
        }
      
    }//-- oDataType   match
    
  }//-- def getDfArrayType() ---
  
  
  
  def getDfStructType( df: org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],  seqPrimaryKeyColumnNames: Seq[String], oColumnPath: StructType, strColumnNamePrefix: String = "", nLevel: Int =0 ):
  ( Seq[String], String, org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] ) = {
    
    val seqKeyColumns = seqPrimaryKeyColumnNames.map( x => { df.col("`" + x + "`") })
    
    val oField = oColumnPath.fields(0)
    
    val strNextPrefix = if( 0 == strColumnNamePrefix.length) oField.name else strColumnNamePrefix + "." + oField.name
    
    val oFieldColumn = if( 0 == strColumnNamePrefix.length) df.col("`" + oField.name + "`") else df.col("`" + strColumnNamePrefix + "`.`" + oField.name + "`").as(strNextPrefix)
    
    val oDataType: DataType = oField.dataType
    
    val seqAllColumns = seqKeyColumns :+ oFieldColumn
    
    val newDf = df.select( seqAllColumns: _* )
    
    
    oDataType   match {
        case StructType(fields)         => {
          
          val oResDf = Comparator.getDfStructType( newDf, seqPrimaryKeyColumnNames, oDataType.asInstanceOf[StructType], strNextPrefix, nLevel+1 )
          
          
          oResDf
        }
        
        case ArrayType(elementType, _)  => {
          
          val oResDf = Comparator.getDfArrayType( newDf, seqPrimaryKeyColumnNames, oDataType.asInstanceOf[ArrayType], strNextPrefix, nLevel+1 )
          
          oResDf
        }
        
        case _                          => {
          
          (seqPrimaryKeyColumnNames, strNextPrefix, newDf)
        }
      
    }//-- oDataType   match
    
  }//----------------- def getDf() ---
  
  
  
  
  
  def checkPrimaryKey( seqPrimaryKeyColumnNames: Seq[String], df: org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] ): Boolean =
  {
    val columns   = seqPrimaryKeyColumnNames.map( x => { df.col("`" + x + "`") } )
    df.sparkSession.sparkContext.setJobGroup("Check primary key", "Select primary key df ( " + seqPrimaryKeyColumnNames + " ) " )
    val primaryDf = df.select( columns: _* )
    
    df.sparkSession.sparkContext.setJobGroup("Check primary key", "Count primary key df" )
    val nCount = primaryDf.count
    
    df.sparkSession.sparkContext.setJobGroup("Check primary key", "Count distinct primary key df" )
    
    val nCountDistinct = primaryDf.distinct.count
    
    nCount.equals( nCountDistinct )
    
    
  }//----------------------------------------------------------------------------------------- def checkPrimaryKey() ---
  
  
  
  
  def getSchemaDiff( 
      oOriginalDf:    org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ], 
      oTransformedDf: org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] )
  : SchemaDiff =
  {
    val seqOriginalDfPaths:    Seq[StructType] = Comparator.getFlatColumnSchema( oOriginalDf )
    val seqTransformedDfPaths: Seq[StructType] = Comparator.getFlatColumnSchema( oTransformedDf )
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "Val seqOriginalDfPaths.size:      " + seqOriginalDfPaths.size)
    println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "Val seqTransformedDfPaths.size:   " + seqTransformedDfPaths.size)
    
    
    val seqTypeChangedOriginalDfPaths = seqOriginalDfPaths.diff( seqTransformedDfPaths )
    val seqAllTransformedDfPaths      = seqTransformedDfPaths.diff( seqOriginalDfPaths )
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "Val seqTypeChangedOriginalDfPaths.size:  " + seqTypeChangedOriginalDfPaths.size)
    println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "Val seqAllTransformedDfPaths.size:       " + seqAllTransformedDfPaths.size)
    
    val seqOriginalUnchangedDfPaths     = seqOriginalDfPaths.diff( seqTypeChangedOriginalDfPaths )
    val seqTransformedUnchangedDfPaths  = seqTransformedDfPaths.diff( seqAllTransformedDfPaths )
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "Val seqOriginalUnchangedDfPaths.size:    " + seqOriginalUnchangedDfPaths.size)
    println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "Val seqTransformedUnchangedDfPaths.size: " + seqTransformedUnchangedDfPaths.size)
    
    val seqZeroDiff1 = seqOriginalUnchangedDfPaths.diff( seqTransformedUnchangedDfPaths )
    val seqZeroDiff2 = seqTransformedUnchangedDfPaths.diff( seqOriginalUnchangedDfPaths )
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "Val seqZeroDiff1.size:  " + seqZeroDiff1.size)
    println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "Val seqZeroDiff2.size:  " + seqZeroDiff2.size)
    
    if (( 0 != seqZeroDiff1.size ) || ( 0 != seqZeroDiff2.size ))
    {
      println( "de.mindlab.spark_schema_transformer.row_iterator.getSchemaDiff(): " + "ERROR: Internal Error: Can not calculate unchanged columns!" )
      SchemaDiff( true, null, null, null )
    }
    else
    {
      SchemaDiff( false, seqTypeChangedOriginalDfPaths, seqAllTransformedDfPaths, seqOriginalUnchangedDfPaths )
    }
  }
  
  
  
  def getFlatColumnSchema( df: org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ]): 
  Seq[StructType] = 
  {
    Comparator.getFlatColumnSchemaStructType(df.schema)
  }//-- def getFlatColumnSchema
  
  
  def getFlatColumnSchemaArrayType( 
      oArrayType:     org.apache.spark.sql.types.ArrayType,
      nLevel:      Int            = 0
      ): 
  Seq[ArrayType] = 
  {
      val oDataType: DataType = oArrayType.elementType
      
      oDataType match {
        
        case StructType(fields)         => {
          
          val seqRes: Seq[StructType] = Comparator.getFlatColumnSchemaStructType( oDataType.asInstanceOf[StructType], nLevel +1 )
          
          
          seqRes.map( oStructTypeChild => {
            
            new ArrayType( oStructTypeChild, oArrayType.containsNull)
            
            
          })
          
        }
        case ArrayType(elementType, _)  => {
          
          val seqRes: Seq[ArrayType] = Comparator.getFlatColumnSchemaArrayType( oDataType.asInstanceOf[ArrayType], nLevel +1 )
          
          seqRes.map( oArrayTypeChild => {
            
            new ArrayType( oArrayTypeChild, oArrayType.containsNull)
            
            
          })
          
        }
        case _                          => {
          
          Seq( new ArrayType( oDataType, oArrayType.containsNull) )
          
        }
        
      }
      
      
    
    
    
  }
  
  
  
  def getFlatColumnSchemaStructField( 
      oField:     org.apache.spark.sql.types.StructField,
      nLevel:      Int            = 0
      ): 
  Seq[StructField] = 
  {
      val oDataType: DataType = oField.dataType
      val strName:   String   = oField.name
      
      oDataType match {
        
        case StructType(fields)         => {
          
          val seqChilds: Seq[ StructType ] = Comparator.getFlatColumnSchemaStructType( oDataType.asInstanceOf[StructType], nLevel +1 )
          
          seqChilds.map( oStructType => {
            
            new StructField(oField.name, oStructType, oField.nullable, oField.metadata)
            
          })
          
          
        }
        
        case ArrayType(elementType, _)  => {
          
          val seqChilds: Seq[ArrayType] = Comparator.getFlatColumnSchemaArrayType( oDataType.asInstanceOf[ArrayType], nLevel +1 )
          
          seqChilds.map( oArrayType => {
            
            new StructField(oField.name, oArrayType, oField.nullable, oField.metadata)
            
          })
          
  
        }
        
        case _                          => {
          
          Seq( new StructField(oField.name, oDataType, oField.nullable, oField.metadata) )
        }
        
      }//-- oDataType match {
    
  }//------------------------------------------------------------------------------------- def getFlatColumnSchema() ---
  
  
  def getFlatColumnSchemaStructType( 
      oSchema:     org.apache.spark.sql.types.StructType,
      nLevel:      Int            = 0
      ): 
  Seq[StructType] = 
  {
    println( "de.mindlab.spark_schema_transformer.row_iterator.Comparator.getFlatColumnSchema( nLevel = " + nLevel + "): " + "START" )
    println( "de.mindlab.spark_schema_transformer.row_iterator.Comparator.getFlatColumnSchema( nLevel = " + nLevel + "): " + "Param oSchema.typeName: " + oSchema.typeName )
    
    val nNumberOfFields    = oSchema.length
    var nCurrentFieldIndex = 0;
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.Comparator.getFlatColumnSchema( nLevel = " + nLevel + "): " + "Val nNumberOfFields: " + nNumberOfFields )
    
    val seqStructTypes: Seq[StructType] = oSchema.map( oField => 
    {
      
      val seqResultingFields: Seq[StructField] = Comparator.getFlatColumnSchemaStructField( oField, nLevel+1 )
      
      seqResultingFields.map( oResultingField => {
        
        StructType(Seq( oResultingField ))
        
        
      } )
      
      
      
      
      //nCurrentFieldIndex = nCurrentFieldIndex +1
      
    }).flatten
    
    
    
    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.Comparator.getFlatColumnSchema( nLevel = " + nLevel + "): " + "END" )
    seqStructTypes
  }//-- def getFlatColumnSchema
  
  
  
  
  
}