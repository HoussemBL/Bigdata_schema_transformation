package de.mindlab.spark_schema_transformer.row_iterator

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
import de.mindlab.spark_schema_transformer.row_iterator.functions._
import de.mindlab.spark_schema_transformer.row_iterator.definitions._
import de.mindlab.spark_schema_transformer.row_iterator.utils._



//AnyDataType                      //An AbstractDataType that matches any concrete data types.
//ArrayType                       I
//BinaryType                      I//The data type representing Array[Byte] values.
//BooleanType                     I//The data type representing Boolean values.
//ByteType                        I//The data type representing Byte values.
//CalendarIntervalType             //The data type representing calendar time intervals.
//CharType                         //Hive char type.
//DataType                        I//The base type of all Spark SQL data types.
//DataTypes                        //To get/create specific data type, users should use singleton objects and factory methods provided by this class.
//DateType                        I//A date type, supporting "0001-01-01" through "9999-12-31".
//Decimal                          //A mutable implementation of BigDecimal that can hold a Long if values are small enough.
//Decimal.DecimalAsIfIntegral$     //A Integral evidence parameter for Decimals.
//Decimal.DecimalIsFractional$     //A Fractional evidence parameter for Decimals.
//DecimalType                      //The data type representing java.math.BigDecimal values.
//DecimalType.Expression$	 
//DecimalType.Fixed$	 
//DoubleType                      I//The data type representing Double values.
//FloatType                        //The data type representing Float values.
//HiveStringType                   //A hive string type for compatibility.
//IntegerType                     I//The data type representing Int values.
//LongType                        I//The data type representing Long values.
//MapType                         I//The data type for Maps.
//Metadata                         //Metadata is a wrapper over Map[String, Any] that limits the value type to simple ones: Boolean, Long, Double, String, Metadata, Array[Boolean], Array[Long], Array[Double], Array[String], and Array[Metadata].
//MetadataBuilder                  //Builder for Metadata.
//NullType                        I//The data type representing NULL values.
//NumericType                      //Numeric data types.
//ObjectType	 
//ShortType                       I//The data type representing Short values.
//StringType                      I//The data type representing String values.
//StructField                     I//A field inside a StructType.
//StructType                      I//A StructType object can be constructed by
//TimestampType                   I//The data type representing java.sql.Timestamp values.
//UDTRegistration                  //This object keeps the mappings between user classes and their User Defined Types (UDTs).
//VarcharType                      //Hive varchar type.








class SchemaMerger {
  
}


object SchemaMerger {
  
//  def checkTransformationDefinition( 
//      sparkSession:               SparkSession,
//      oTransformationDefinition:  TransformationDefinition,
//      df:                         org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
//      ): ( Boolean, ColumnTransformationDefinition ) = {
//    
//      val seqResults: ( Boolean, ColumnTransformationDefinition ) = oTransformationDefinition.seqColumnTransformationDefinitions.map( oColumnTransformationDefinition => {
//      
//        val bIsValid: Boolean = true
//        
//        
//        
//        
//      (bIsValid, oColumnTransformationDefinition)
//    })
//    
//    
//  }
//  
//  
//  def checkColumnTransformationDefinition( 
//      sparkSession:                     SparkSession,
//      oColumnTransformationDefinition:  ColumnTransformationDefinition,
//      df:                               org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
//      ): ( Boolean, ColumnTransformationDefinition ) = {
//    
//      val seqResults: ( Boolean, ColumnTransformationDefinition ) = oTransformationDefinition.seqColumnTransformationDefinitions.map( oColumnTransformationDefinition => {
//      
//        val bIsValid: Boolean = true
//        
//        
//        
//        
//      (bIsValid, oColumnTransformationDefinition)
//    })
//    
//    
//  }
//  
  
  
  
  var m_oGlobalArrayDF:  org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = null
  var m_oGlobalStructDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = null
  var m_oCurrentCol:     org.apache.spark.sql.Column                            = null
  
  
  
  
  def innerTransformDf (
      df:                             org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      seqFirstLevelColumnNames:       Seq[String],
      oRecTransformationDef:          (org.apache.spark.sql.expressions.UserDefinedFunction, ( org.apache.spark.sql.Row ) => org.apache.spark.sql.Row, org.apache.spark.sql.types.DataType )
      ): org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] = {
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.innerTransformDf(): " + "START" )
    
    val listFirstLevelColumns: List[org.apache.spark.sql.Column] = seqFirstLevelColumnNames.map(x => { df.col("`" + x + "`") }).toList
    
    val seqOriginalFieldNames: Seq[String] = df.schema.fieldNames.toSeq
    
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.innerTransformDf(): " + "Val listFirstLevelColumns: " + listFirstLevelColumns )
    
    val firstLevelDf = df.select( listFirstLevelColumns: _* )
    
    
    val oRes = oRecTransformationDef
    
    
    val listColumns: List[org.apache.spark.sql.Column] = firstLevelDf.schema.fieldNames.map(x => { df.col("`" + x + "`") }).toList
    val oColIn = struct( listColumns: _* )
    val resDf = df.withColumn("res", oRes._1(oColIn))  //.select(oRes._1(oColIn).as("res"))
    
    
    val listAllColumns: List[org.apache.spark.sql.Column] = seqOriginalFieldNames.map(x => { if ( seqFirstLevelColumnNames.contains(x)  ) resDf.col("res.`" + x + "`") else resDf.col("`" + x + "`") }).toList
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.innerTransformDf(): " + "Val listAllColumns: " + listAllColumns )
    
    val resResDf = resDf.select( listAllColumns: _* )
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.innerTransformDf(): " + "END" )
    resResDf
  }
  
  def prepareTransformation(
      df:                             org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      oTransformationDefinition:      TransformationDefinition 
      ): ( Seq[String], (org.apache.spark.sql.expressions.UserDefinedFunction, ( org.apache.spark.sql.Row ) => org.apache.spark.sql.Row, org.apache.spark.sql.types.DataType ) )= 
    {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.prepareTransformation(): " + "START" )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.prepareTransformation(): " + "Param oTransformationDefinition: " + oTransformationDefinition )
    
    val seqFirstLevelColumnNames: Seq[String]                       = SchemaMerger.getSrcAndTgtPathFirstLevelNames( oTransformationDefinition )
    val listFirstLevelColumns:    List[org.apache.spark.sql.Column] = seqFirstLevelColumnNames.map(x => { df.col("`" + x + "`") }).toList
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.prepareTransformation(): " + "Val listFirstLevelColumns: " + listFirstLevelColumns )
    
    
    val seqOriginalFieldNames: Seq[String] = df.schema.fieldNames.toSeq
    
    val firstLevelDf = df.select( listFirstLevelColumns: _* )
    
    val oRes: (org.apache.spark.sql.expressions.UserDefinedFunction, ( org.apache.spark.sql.Row ) => org.apache.spark.sql.Row, org.apache.spark.sql.types.DataType ) = SchemaMerger.generateTransformationUdf( oTransformationDefinition, firstLevelDf.schema )

    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.prepareTransformation(): " + "END" )
    
    ( seqFirstLevelColumnNames, oRes )
    
    }
  
  def transformDf (
      df:                             org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      oTransformationDefinition:      TransformationDefinition 
      ): org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformDf(): " + "START" )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformDf(): " + "Param oTransformationDefinition: " + oTransformationDefinition )
    
//    val seqFirstLevelColumnNames: Seq[String] = SchemaMerger.getSrcAndTgtPathFirstLevelNames( oTransformationDefinition )
//    
//    
//    
//    
//    
//    val listFirstLevelColumns: List[org.apache.spark.sql.Column] = seqFirstLevelColumnNames.map(x => { df.col("`" + x + "`") }).toList
//    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformDf(): " + "Val listFirstLevelColumns: " + listFirstLevelColumns )
//    
//    
//    val seqOriginalFieldNames: Seq[String] = df.schema.fieldNames.toSeq
//    
//    val firstLevelDf = df.select( listFirstLevelColumns: _* )
//    
//    val oRes: (org.apache.spark.sql.expressions.UserDefinedFunction, ( org.apache.spark.sql.Row ) => org.apache.spark.sql.Row, org.apache.spark.sql.types.DataType ) = SchemaMerger.generateTransformationUdf( oTransformationDefinition, firstLevelDf.schema )
//    
    
    val prepareRes = SchemaMerger.prepareTransformation(df, oTransformationDefinition)
    
//    val resResDf = SchemaMerger.innerTransformDf(df, seqFirstLevelColumnNames, oRes)
    val resResDf = SchemaMerger.innerTransformDf(df, prepareRes._1, prepareRes._2)
    
    
//    val listColumns: List[org.apache.spark.sql.Column] = firstLevelDf.schema.fieldNames.map(x => { df.col("`" + x + "`") }).toList
//    val oColIn = struct( listColumns: _* )
//    val resDf = df.withColumn("res", oRes._1(oColIn))  //.select(oRes._1(oColIn).as("res"))
//    
////    val listNewColumns: List[org.apache.spark.sql.Column] = firstLevelDf.schema.fieldNames.map(x => { resDf.col("res.`" + x + "`") }).toList
//    
//    val seqOriginalFieldNames: Seq[String] = df.schema.fieldNames.toSeq
//
//    
//    val listAllColumns: List[org.apache.spark.sql.Column] = seqOriginalFieldNames.map(x => { if ( seqFirstLevelColumnNames.contains(x)  ) resDf.col("res.`" + x + "`") else resDf.col("`" + x + "`") }).toList
//    
////    val resResDf = resDf.select( (listNewColumns ++ listOldColumns ): _* )
//    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformDf(): " + "Val listAllColumns: " + listAllColumns )
//    
//    val resResDf = resDf.select( listAllColumns: _* )
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformDf(): " + "END" )
    resResDf
  }
  
  
  
  
  def convertToRecursivePathDefinitionFromArray( 
      oTransformationDefinition:      TransformationDefinition,
      oArrayType:                     org.apache.spark.sql.types.ArrayType, 
      nLevel:                         Int                      = 0
  ): (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinitionFromArray( nLevel = " + nLevel + "): " + "START" )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinitionFromArray( nLevel = " + nLevel + "): " + "Param oArrayType.typeName: " + oArrayType.typeName )
    
    val oDataType = oArrayType.elementType
    
    val oRes: (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = oDataType match {
      case StructType(fields)         => {
        val oCurrRes: (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = SchemaMerger.convertToRecursivePathDefinition( 
            oTransformationDefinition,
            oDataType.asInstanceOf[StructType],
            nLevel + 1
        )
        
        (oCurrRes._1, ArrayType( oCurrRes._2 ))
      }
      case ArrayType(elementType, _)  => {
        
        val oCurrRes: (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = SchemaMerger.convertToRecursivePathDefinitionFromArray( 
            oTransformationDefinition,
            oDataType.asInstanceOf[ArrayType],
            nLevel + 1
        )
        
        (oCurrRes._1, ArrayType( oCurrRes._2 ))
      }
      case _                          => {
        (Seq(), null)
      }
    }
    
    
    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinitionFromArray( nLevel = " + nLevel + "): " + "END" )
    
    oRes
  }//-- def convertToRecursivePathDefinitionFromArray(...)
  
  
  
  def getNewValueAndDataType( oRowOriginal: org.apache.spark.sql.Row, value: Any, oDataType: org.apache.spark.sql.types.DataType, oLeaf: ColumnTransformationDefinition): (Any, DataType) = {
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "START" )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Param value: "     + value     )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Param oDataType: " + oDataType )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Param oLeaf: "     + oLeaf     )
    
    
    val strFunctionName:      String                = oLeaf.strFunctionName
    val seqSourceDefinition:  Seq[SourceDefinition] = oLeaf.seqSourceDefinition
    
    
    
    
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Val strFunctionName:     " + strFunctionName)
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Val seqSourceDefinition: " + seqSourceDefinition)
    
    val fncDataTypeGenerator = TransformationFunctions.getTransformation( strFunctionName )
    val fncValueGenerator    = TransformationFunctions.getFnc( strFunctionName )
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Val fncDataTypeGenerator: " + fncDataTypeGenerator )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Val fncValueGenerator: "    + fncValueGenerator    )
    
    val seqSrcValues = seqSourceDefinition.map( x => { if ( null != x.oConst ) x.oConst.strValue else {
      if ( x.oPath != null ) { x.oPath.fncValueFetching( oRowOriginal ) } else null
    } } )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Val seqSrcValues: " + seqSrcValues)

    
    
    val oRes: (Any, DataType) = (fncValueGenerator(seqSrcValues, value), fncDataTypeGenerator(seqSrcValues, oDataType))
    
//    val oRes: (Any, DataType) = ( value, oDataType )
    
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "Var oRes: "     + oRes     )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewValueAndDataType(): " + "END" )
    
    oRes
    
  }//-- def getNewValueAndDataType
  
  
  
  def getNewDataType( oDataType: org.apache.spark.sql.types.DataType, oLeaf: ColumnTransformationDefinition ): org.apache.spark.sql.types.DataType = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewDataType(): " + "START" )
    
    
//    oTargetPathDefinition:       PathDefinition,
//    strFunctionName:             String,
//    seqSourceDefinition:         Seq[SourceDefinition] = Seq(),
//    bOverwriteTarget:            Boolean = false,
//    bDataParentExistenceNeeded:  Boolean = true

//    oDataType match {
//      case ArrayType(elementType2, _)  => 
//    }//-- oDataType match
    
    
    
    
    val strFunctionName:      String                = oLeaf.strFunctionName
    val seqSourceDefinition:  Seq[SourceDefinition] = oLeaf.seqSourceDefinition
    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewDataType(): " + "Val strFunctionName:     " + strFunctionName)
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewDataType(): " + "Val seqSourceDefinition: " + seqSourceDefinition)
    
    val fncDataTypeGenerator = TransformationFunctions.getTransformation( strFunctionName )
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewDataType(): " + "Val fncDataTypeGenerator: " + fncDataTypeGenerator)
    
    val seqSrcValues = seqSourceDefinition.map( x => { if ( null != x.oConst ) x.oConst.strValue else null } )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewDataType(): " + "Val seqSrcValues: " + seqSrcValues)
    
    val oRes = fncDataTypeGenerator(seqSrcValues, oDataType)
    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewDataType(): " + "Val oRes: " + oRes)
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getNewDataType(): " + "END" )
    
    
    oRes
  }//-- getNewDataType(...) ---
  
  
  
  def convertToRecursivePathDefinition( 
      oTransformationDefinition:      TransformationDefinition,
      oSchema:                        org.apache.spark.sql.types.StructType, 
      nLevel:                     Int                      = 0
  ): (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "START" )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Param oSchema.typeName: " + oSchema.typeName )
    
    val seqChilds:     Seq[(PathToken, TransformationDefinition)] = TransformationDefinition.getFirstPathTokens( oTransformationDefinition )
    val seqFieldNames: Seq[ String ] = seqChilds.map( x => { x._1.strToken } )
    
    var seqPairRes: Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)] = Seq()
    
    
    var nIndex = 0
    val seqFields = oSchema.map( oFieldOriginal => {
      
      val strFieldName:              String  = oFieldOriginal.name
      val bFieldShouldBeTransformed: Boolean = seqFieldNames.contains(strFieldName)
      
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val strFieldName: " + strFieldName )
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val bFieldShouldBeTransformed: " + bFieldShouldBeTransformed )
      
      val oResField: StructField = if ( bFieldShouldBeTransformed )
      {
        val nFieldIndex    = nIndex
        val oLocalDataType = oFieldOriginal.dataType
        // val seqChilds:     Seq[(PathToken, TransformationDefinition)]
        
        val pairTransformationDefinition: (PathToken, TransformationDefinition) = seqChilds.filter( x => {x._1.strToken.equals( strFieldName )} ).head
        
        val pairLeafAndBranch: (ColumnTransformationDefinition, TransformationDefinition) = TransformationDefinition.getLeafAndBranch( pairTransformationDefinition._2 )
        
        val bLeafExists:   Boolean = (null != pairLeafAndBranch._1)
        val bBranchExists: Boolean = (null != pairLeafAndBranch._2)
        
        val oLeaf:   ColumnTransformationDefinition = pairLeafAndBranch._1
        val oBranch:       TransformationDefinition = pairLeafAndBranch._2
          
        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val nFieldIndex: "                  + nFieldIndex )
        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val pairTransformationDefinition: " + pairTransformationDefinition )
        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val pairLeafAndBranch: "            + pairLeafAndBranch )
        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val bLeafExists: "                  + bLeafExists )
        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val bBranchExists: "                + bBranchExists )
        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val oLeaf: "                        + oLeaf )
        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val oBranch: "                      + oBranch )
        
        
        val oNewLocalDataType = if ( bLeafExists )
        {
          SchemaMerger.getNewDataType( oLocalDataType, oLeaf )
        }
        else
        {
          oLocalDataType
        }
        
        
        val oFieldTransformed: StructField = 
            if ( bBranchExists )
            {
                oNewLocalDataType match {
                case StructType(fields)          => {
                  
                  //oLocalDataType.asInstanceOf[StructType]
                  
                  
                  val pairCurrRes: (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = SchemaMerger.convertToRecursivePathDefinition( 
                    oBranch,
                    oNewLocalDataType.asInstanceOf[StructType], 
                    nLevel +1 )
                    
                  val oResRecursivePathDefinition: RecursivePathDefinition = RecursivePathDefinition(
                      pairTransformationDefinition._1,
                      pairCurrRes._2,
                      pairCurrRes._1.map(x => {x._1}),
                      if ( bLeafExists ) oLeaf else null,
                      !((pairCurrRes._1.map(x => {x._1}).map(x => {x.bDataParentExistenceNeeded}).filter( x => !x ).size > 0 ) || ( if ( bLeafExists ) oLeaf.bDataParentExistenceNeeded else false ))
                  )
                  
                  seqPairRes = seqPairRes :+ ( oResRecursivePathDefinition  , oNewLocalDataType )
                    
                      
                      
                  StructField(oFieldOriginal.name, pairCurrRes._2, oFieldOriginal.nullable, oFieldOriginal.metadata )
                }
                case ArrayType(elementType2, _)  => {
                  val oArrayDataType = oNewLocalDataType.asInstanceOf[ArrayType].elementType
                  
                  val pairCurrRes: (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = SchemaMerger.convertToRecursivePathDefinitionFromArray( 
                    oBranch,
                    oNewLocalDataType.asInstanceOf[ArrayType], 
                    nLevel +1 )
                  
                    
                  val oResRecursivePathDefinition: RecursivePathDefinition = RecursivePathDefinition(
                      pairTransformationDefinition._1,
                      pairCurrRes._2,
                      pairCurrRes._1.map(x => {x._1}),
                      if ( bLeafExists ) oLeaf else null,
                      !((pairCurrRes._1.map(x => {x._1}).map(x => {x.bDataParentExistenceNeeded}).filter( x => !x ).size > 0 ) || ( if ( bLeafExists ) oLeaf.bDataParentExistenceNeeded else false ))
                  )

                    
                    
                    
                  seqPairRes = seqPairRes :+ ( oResRecursivePathDefinition  , pairCurrRes._2 )
                  
                  StructField(oFieldOriginal.name, pairCurrRes._2, oFieldOriginal.nullable, oFieldOriginal.metadata )
                  }
                case _                           => {
                  
                  StructField(oFieldOriginal.name, oNewLocalDataType, oFieldOriginal.nullable, oFieldOriginal.metadata )
                  }
              }
            }//-- if ( bBranchExists )
            else
            {
              // seqPairRes: Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)]
              
//              case class RecursivePathDefinition(
//                  oPathToken:                       PathToken                              = null,
//                  oTargetDataType:                  org.apache.spark.sql.types.DataType    = null, 
//                  seqChilds:                        Seq[ RecursivePathDefinition ]         = Seq(),
//                  oColumnTransformationDefinition:  ColumnTransformationDefinition         = null,
//                  bDataParentExistenceNeeded:       Boolean                                = true
//              )
              
              val oResRecursivePathDefinition: RecursivePathDefinition = RecursivePathDefinition(
                  pairTransformationDefinition._1,
                  oNewLocalDataType,
                  Seq(),
                  oLeaf,
                  oLeaf.bDataParentExistenceNeeded
              )
              
              
              
              seqPairRes = seqPairRes :+ ( oResRecursivePathDefinition  , oNewLocalDataType )
              StructField(oFieldOriginal.name, oNewLocalDataType, oFieldOriginal.nullable, oFieldOriginal.metadata )
            }
        
        
        oFieldTransformed
        
      }
      else
      {
        oFieldOriginal
      }
      
      
      nIndex = nIndex + 1
      
      oResField
    })
    
    
    
    val oRes = ( seqPairRes, StructType(seqFields) )
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "Val oRes: " + oRes )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nLevel = " + nLevel + "): " + "END" )
    oRes
  }//-- def convertToRecursivePathDefinition
  
  
  
  
  
  
//  def convertToRecursivePathDefinition( 
//      oTransformationDefinition:      TransformationDefinition,
//      oSchema:                        org.apache.spark.sql.types.DataType, //  org.apache.spark.sql.types.StructType, 
//      nPathTokenLevel:                Int                      = 0
//  ): (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = {
//    
//    
////    oPathToken:                       PathToken                      = null,
////    seqChilds:                        Seq[ RecursivePathDefinition ] = Seq(),
////    oColumnTransformationDefinition:  ColumnTransformationDefinition = null
//    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "START" )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Param oSchema.typeName: " + oSchema.typeName )
//    
//    val seqChilds: Seq[(PathToken, TransformationDefinition)] = TransformationDefinition.getFirstPathTokens( oTransformationDefinition )
//    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Var seqChilds: " + seqChilds )
//    
//    val seqRes: Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)] = seqChilds.map( pairInnerTransformation => {
//      
//      val nCurrentPathToken:                PathToken                = pairInnerTransformation._1
//      val oCurrentTransformationDefinition: TransformationDefinition = pairInnerTransformation._2
//      
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Var nCurrentPathToken: " + nCurrentPathToken )
//      
//      
//      
//      var oTargetDataType: org.apache.spark.sql.types.DataType = null
//      
//      val oDataType = oSchema match {
//        
//        case ArrayType(elementType, _)   => { 
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "case ArrayType(elementType, _)")
//                                              oTargetDataType = oSchema.asInstanceOf[ArrayType]
//                                              oSchema.asInstanceOf[ArrayType].elementType 
//                                            }
//        case StructType(fields)          => {
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "case StructType(fields)")
//                                              
//                                              val nFieldIndex    = oSchema.asInstanceOf[StructType].fieldIndex( nCurrentPathToken.strToken )
//                                              val oField         = oSchema.asInstanceOf[StructType].fields(nFieldIndex)
//                                              val oLocalDataType = oField.dataType
//                                              
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "case StructType(fields): nFieldIndex: " + nFieldIndex )
//                                              
//                                              
//                                              oLocalDataType match {
//                                                case ArrayType(elementType2, _)  => {oLocalDataType.asInstanceOf[ArrayType].elementType}
//                                                case _                           => {oLocalDataType}
//                                              }
//                                              
//                                              
//                                              //oLocalDataType
//                                            }
//        case _                           => { 
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "case _")
//                                              oSchema 
//                                            }
//        
//      }//-- val oDataType = oSchema match....
//      
//      
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Var oDataType:         " + oDataType )
//      
//      
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Var oCurrentTransformationDefinition.seqColumnTransformationDefinitions.size: " + oCurrentTransformationDefinition.seqColumnTransformationDefinitions.size )
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Var oCurrentTransformationDefinition.seqColumnTransformationDefinitions.head.oTargetPathDefinition.seqPathTokens.size: " + oCurrentTransformationDefinition.seqColumnTransformationDefinitions.head.oTargetPathDefinition.seqPathTokens.size )
//      
//      
//      val seqCildChilds:   ( Seq[ (RecursivePathDefinition, org.apache.spark.sql.types.DataType) ], org.apache.spark.sql.types.DataType) = 
//      if ( oCurrentTransformationDefinition.seqColumnTransformationDefinitions.size == 1 )
//      {
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "oCurrentTransformationDefinition.seqColumnTransformationDefinitions.size == 1" )
//        
//        if ( oCurrentTransformationDefinition.seqColumnTransformationDefinitions.head.oTargetPathDefinition.seqPathTokens.size == 0 )
//        {
//          
//          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "oCurrentTransformationDefinition.seqColumnTransformationDefinitions.head.oTargetPathDefinition.seqPathTokens.size == 0" )
//          
//          ( Seq( 
////              RecursivePathDefinition(
////                  nCurrentPathToken, //oCurrentTransformationDefinition.seqColumnTransformationDefinitions.head.oTargetPathDefinition.seqPathTokens.head,
////                  Seq(),
////                  oCurrentTransformationDefinition.seqColumnTransformationDefinitions.head
////               )
//          ), null)
//          
//        }
//        else
//        {
//          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "oCurrentTransformationDefinition.seqColumnTransformationDefinitions.head.oTargetPathDefinition.seqPathTokens.size != 0" )
//          
//          val seqChildRecursivePathDefinitions: (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = 
//            SchemaMerger.convertToRecursivePathDefinition( 
//              oCurrentTransformationDefinition,
//              oDataType,
//              nPathTokenLevel +1
//          )
//          
//          seqChildRecursivePathDefinitions //._1.map( x => x._1 )
//          
//        }
//      }//-- val seqCildChilds:   ( Seq[ (RecursivePathDefinition, org.apache.spark.sql.types.DataType) ], org.apache.spark.sql.types.DataType) = ...
//      else
//      {
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "oCurrentTransformationDefinition.seqColumnTransformationDefinitions.size != 1" )
//        
//        val seqChildRecursivePathDefinitions: (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = 
//          SchemaMerger.convertToRecursivePathDefinition( 
//              oCurrentTransformationDefinition,
//              oDataType,
//              nPathTokenLevel +1
//          )
//          
//          seqChildRecursivePathDefinitions //._1.map( x => x._1 )
//      }//-- val seqCildChilds:   ( Seq[ (RecursivePathDefinition, org.apache.spark.sql.types.DataType) ], org.apache.spark.sql.types.DataType) = ...
//      
//      
//      //----------------------------------------------------------------------------------------------------------------
//      
//      if ( seqCildChilds._1.size > 0 )
//      {
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "seqCildChilds._1.size > 0" )
//        
//        
//        oTargetDataType = oSchema match {
//      
//          case ArrayType(elementType, _)   => { 
//                                                println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result data type: case ArrayType(elementType, _)")
//                                                
//                                                if ( seqCildChilds._1.size == 1 )
//                                                {
//                                                  //seqCildChilds._1.head._1.oTargetDataType
//                                                  seqCildChilds._2
////                                                  ArrayType( seqCildChilds.head.oTargetStructField, oSchema.asInstanceOf[ArrayType].containsNull )
//                                                }
//                                                else
//                                                {
//                                                  println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "ERROR: Result data type: case ArrayType(elementType, _): Has many children: " + seqCildChilds._1.size)
//                                                  null
//                                                }
//                                              }
//    
//          case StructType(fields)          => {
//                                                println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result data type: case StructType(fields)")
//                                                
//                                                val nFieldIndex = oSchema.asInstanceOf[StructType].fieldIndex( nCurrentPathToken.strToken )
//                                                val oField      = oSchema.asInstanceOf[StructType].fields(nFieldIndex)
//                                                val oLocalDataType = oField.dataType
//                                                
//                                                println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result data type: case StructType(fields): nFieldIndex: " + nFieldIndex )
//                                                
//                                                
//                                                oLocalDataType match {
//                                                  case ArrayType(elementType2, _)  => {
//                                                
//                                                      val oLocalTargetDataType = ArrayType( seqCildChilds._2/*.head._1.oTargetDataType*/, oLocalDataType.asInstanceOf[ArrayType].containsNull )
//                                                      val oLocalField          = StructField(oField.name, oLocalTargetDataType, oField.nullable, oField.metadata )
//                                                      
//                                                      StructType( Seq( oLocalField ) )
//                                                    }
//                                                  case _                           => {
//                                                    oLocalDataType
//                                                    }
//                                                }
//                                                
//                                                
//                                                //oLocalDataType
//                                              }
//          case _                           => { 
//                                                println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result data type: case _")
//                                                oSchema 
//                                              }
//          
//        }
//        
//        (
//        RecursivePathDefinition(
//                    nCurrentPathToken,
//                    oTargetDataType,
//                    
//                    seqCildChilds._1.map(x => x._1)
//                 ),
//                 oTargetDataType
//        )
//      }
//      else
//      {
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "seqCildChilds._1.size <= 0" )
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Val oCurrentTransformationDefinition.seqColumnTransformationDefinitions: " + oCurrentTransformationDefinition.seqColumnTransformationDefinitions )
//        
//        val nFieldIndex = oSchema.asInstanceOf[StructType].fieldIndex( nCurrentPathToken.strToken )
//        val oField      = oSchema.asInstanceOf[StructType].fields(nFieldIndex)
//        
//        (
//        RecursivePathDefinition(
//                  nCurrentPathToken,
//                  StructType(Seq(StructField(oField.name, ArrayType( oDataType, true ), oField.nullable, oField.metadata ))),
//                  seqCildChilds._1.map(x => x._1),
//                  oCurrentTransformationDefinition.seqColumnTransformationDefinitions.head
//               ),
//               ArrayType( oDataType, true )
//        )
//               
//      }
//    }) //-- val seqRes: Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)]
//    
//    
//    
//    val oOutSchema = oSchema match {
//      
//      case ArrayType(elementType, _)   => { 
//                                            println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result schema: case ArrayType(elementType, _)")
//                                            
//                                            if ( seqRes.size == 1 )
//                                            {
//                                              
//                                              ArrayType( seqRes.head._1.oTargetDataType, oSchema.asInstanceOf[ArrayType].containsNull )
//                                              null
//                                            }
//                                            else
//                                            {
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "ERROR: Result schema: case ArrayType(elementType, _): Has many children: " + seqRes.size)
//                                              null
//                                            }
//                                          }
//
//      case StructType(fields)          => {
//                                            println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result schema: case StructType(fields)")
//                                            
//                                            val listDataTypePairs: List[(Int, String, StructField)] = seqRes.map( oPairLocalRecursivePathDefinition => {
//                                              
//                                              val oLocalRecursivePathDefinition = oPairLocalRecursivePathDefinition._1
//                                              val oLocalRecursiveSchema         = oPairLocalRecursivePathDefinition._2
//                                              
//                                              val nFieldIndex    = oSchema.asInstanceOf[StructType].fieldIndex( oLocalRecursivePathDefinition.oPathToken.strToken )
//                                              
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result schema: case StructType(fields) in listDataTypePairs. Val oLocalRecursivePathDefinition.oPathToken.strToken: " + oLocalRecursivePathDefinition.oPathToken.strToken )
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result schema: case StructType(fields) in listDataTypePairs. Val oLocalRecursivePathDefinition.oTargetDataType: "     + oLocalRecursivePathDefinition.oTargetDataType )
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result schema: case StructType(fields) in listDataTypePairs. Val oLocalRecursiveSchema: "                             + oLocalRecursiveSchema )
//                                              println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result schema: case StructType(fields) in listDataTypePairs. Val nFieldIndex: " + nFieldIndex )
//                                              
//                                              ( nFieldIndex, oLocalRecursivePathDefinition.oPathToken.strToken, oLocalRecursivePathDefinition.oTargetDataType.asInstanceOf[StructType].fields(0) )
//                                              
//                                            }).toList
//                                            
//                                            var listFieldTypes: List[StructField] = List()
//                                            
//                                            val listIndexes: List[Int] = listDataTypePairs.map(x => {x._1})
//                                            
//                                            var nCurrentIndexOut = 0
//                                            
//                                            for( nCurrentIndexOut <- oSchema.asInstanceOf[StructType].indices ){
//                                              
//                                              if ( listIndexes.contains( nCurrentIndexOut ) )
//                                              {
//                                                val oCurrDataType: StructField = listDataTypePairs.filter( x => {x._1.equals( nCurrentIndexOut )} ).head._3
//                                                
//                                                
//                                                listFieldTypes = listFieldTypes ++ List( oCurrDataType )
//                                              }
//                                              else
//                                              {
//                                                listFieldTypes = listFieldTypes ++ List( oSchema.asInstanceOf[StructType].fields(nCurrentIndexOut) )
//                                              }
//                                              
//                                              
//                                            }
//                                            
//                                            
//                                            StructType(listFieldTypes)
//
//                                          }
//      case _                           => { 
//                                            println("de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Result schema: case _")
//                                            oSchema
//                                          }
//      
//    }
//    
//
//    
//    
//    val oResRes = ( seqRes, oOutSchema )
//    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.convertToRecursivePathDefinition( nPathTokenLevel = " + nPathTokenLevel + "): " + "Var oResRes: " + oResRes )
//    
//    oResRes
//  }//  def convertToRecursivePathDefinition ---
  //RecursivePathDefinition
  
  
  
  
  
  def arrayTransformationFnc( 
      oRowOriginal:             org.apache.spark.sql.Row,
      seqIn:                    Seq[Any],
      oSchemaIn:                org.apache.spark.sql.types.DataType,
      oRecursivePathDefinition: RecursivePathDefinition,
      nLevel:                   Int                                 = 0
  ): Seq[Any] = {
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "START" )
    
    val seqRes: Seq[Any] = oSchemaIn match {
      
      case ArrayType(elementType, _)   => {
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "case ArrayType(elementType, _)" )
        
        val oLocalArrayDataType = oSchemaIn.asInstanceOf[ArrayType].elementType
        
        if (null == seqIn)
        {
//          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "case ArrayType(elementType, _): seqIn is null " )
          SchemaMerger.arrayTransformationFnc( 
              oRowOriginal,
              null,
              oLocalArrayDataType,
              oRecursivePathDefinition,
              nLevel + 1
          )

        }
        else
        {
          seqIn.map( oElement => {
//            println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "case ArrayType(elementType, _): oElement: " + oElement )
            SchemaMerger.arrayTransformationFnc(
                oRowOriginal,
                oElement.asInstanceOf[Seq[Any]],
                oLocalArrayDataType,
                oRecursivePathDefinition,
                nLevel + 1
            )
            
          })
          
        }
        
      }//-- case ArrayType(elementType, _)   =>
      case StructType(fields)          => {
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "case StructType(fields)" )
        
        if (null == seqIn)
        {
//          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "case StructType(fields): seqIn is null " )
          
            Seq( 
                SchemaMerger.recTransformationFnc( 
                    oRowOriginal,
                    null,
                    oRecursivePathDefinition.seqChilds,
                    oSchemaIn.asInstanceOf[StructType],
                    false,
                    nLevel + 1
                    )
                    )
          
        }
        else
        {
          seqIn.map( oElement => {
            
//            println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "case StructType(fields): oElement: " + oElement )
            
            SchemaMerger.recTransformationFnc( 
                    oRowOriginal,
                    oElement.asInstanceOf[Row],
                    oRecursivePathDefinition.seqChilds,
                    oSchemaIn.asInstanceOf[StructType],
                    false,
                    nLevel + 1
                    )
          })
        }
        //Seq()
      }
      case _                           => {
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "case _" )
        seqIn.map( oElement => {
          
//          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "case _: oElement: " + oElement )
          
          
        })
        Seq()
        
        
      }
      
    }//-- oSchemaIn match
    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "Val seqRes: " + seqRes )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.arrayTransformationFnc( nLevel = " + nLevel + "): " + "END" )
    
    
    
    seqRes
  }//-- def arrayTransformationFnc(...)
  
//  def fillSources( 
//      oRowIn:                         org.apache.spark.sql.Row,
//      seqRecursivePathDefinitions:    Seq[ RecursivePathDefinition ],
//      oSchemaIn:                      org.apache.spark.sql.types.StructType,
//      nLevel:                         Int                                 = 0
//      ): Unit = 
//  {
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.fillSources( nLevel = " + nLevel + "): " + "START" )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.fillSources( nLevel = " + nLevel + "): " + "Param oRowIn: " + oRowIn )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.fillSources( nLevel = " + nLevel + "): " + "Param seqRecursivePathDefinitions: " + seqRecursivePathDefinitions )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.fillSources( nLevel = " + nLevel + "): " + "Param oSchemaIn: " + oSchemaIn )
//    
//    
//    val listTransformedColumnNames: List[String] = seqRecursivePathDefinitions.map( oRecursivePathDefinition => { oRecursivePathDefinition.oPathToken.strToken } ).toList
//    
//    
//    
//    
//    
//
//    
//    
//    
//  }
  
  
  def recTransformationFnc( 
      oRowOriginal:                   org.apache.spark.sql.Row,
      oRowIn:                         org.apache.spark.sql.Row,
      seqRecursivePathDefinitions:    Seq[ RecursivePathDefinition ],
      oSchemaIn:                      org.apache.spark.sql.types.StructType,
      bShouldFillSourceValues:        Boolean                             = false,
      nLevel:                         Int                                 = 0
      ): org.apache.spark.sql.Row = {
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "START" )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "Param oRowIn: " + oRowIn )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "Param seqRecursivePathDefinitions: " + seqRecursivePathDefinitions )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "Param oSchemaIn: " + oSchemaIn )
    
    val oRowRes: org.apache.spark.sql.Row = {
        
        val listTransformedColumnNames: List[String] = seqRecursivePathDefinitions.map( oRecursivePathDefinition => { oRecursivePathDefinition.oPathToken.strToken } ).toList
        val oSchemaInStruct:            StructType   = oSchemaIn.asInstanceOf[StructType]
        
//        println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ] " )
        
//        if ( bShouldFillSourceValues )
//        {
//          SchemaMerger.fillSources( oRowIn, seqRecursivePathDefinitions, oSchemaIn )
//        }//-- if ( bShouldFillSourceValues )
        
        
        var nIndex = 0
        val seqChildsValues = oSchemaInStruct.map( oFieldOriginal => {
          
          val strFieldName:   String             = oFieldOriginal.name
          
          val bFieldShouldBeTransformed: Boolean = listTransformedColumnNames.contains(strFieldName)
          
//          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: strFieldName: " + strFieldName )
//          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: bFieldShouldBeTransformed: " + bFieldShouldBeTransformed )
          
          val mRes = if ( bFieldShouldBeTransformed )
          {
//            println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: strFieldName: " + strFieldName )
//            println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: bFieldShouldBeTransformed: " + bFieldShouldBeTransformed )
            if (null == oRowIn)
            {
              null
            }
            else
            {
              val oLocalDataType = oFieldOriginal.dataType
              
              val oRecursivePathDefinition: RecursivePathDefinition = seqRecursivePathDefinitions.filter(x => { x.oPathToken.strToken.equals( strFieldName ) }).head
              
              val bIsLeaf      = ( null != oRecursivePathDefinition.oColumnTransformationDefinition )
              val bHasChildren = ( oRecursivePathDefinition.seqChilds.size > 0 )
              
              val bValueExists: Boolean = ( null != oRowIn ) && ( !oRowIn.isNullAt( nIndex ) )
              
//              println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: bIsLeaf: " + bIsLeaf )
//              println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: bHasChildren: " + bHasChildren )
//              println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: bValueExists: " + bValueExists )
              
              
              
              if ( bValueExists )
              {
                val pairValueAndDataType: (Any, DataType) = if (bIsLeaf )
                {
                  val oCurrRes: (Any, DataType) = SchemaMerger.getNewValueAndDataType( oRowOriginal, oRowIn.apply( nIndex ) , oLocalDataType, oRecursivePathDefinition.oColumnTransformationDefinition)
                  
                  
                  oCurrRes
                }
                else
                {
                  ( oRowIn.apply( nIndex ) , oLocalDataType)
                }
                
                val mRowValue    = pairValueAndDataType._1
                val oNewDataType = pairValueAndDataType._2
                
                
                if (!bHasChildren)
                {
                  mRowValue
                }
                else
                {
                  oNewDataType match {
                    
                    case ArrayType(elementType2, _)  => {
//                      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case ArrayType(elementType2, _)] " )
                      
                      val oLocalArrayDataType = oNewDataType.asInstanceOf[ArrayType].elementType
                      val seqInnerRowSeq      = mRowValue.asInstanceOf[Seq[Any]] //if ( bValueExists ) mRowValue.asInstanceOf[Row].getSeq( nIndex ) else null
                      
//                      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case ArrayType(elementType2, _)]: oLocalArrayDataType: " + oLocalArrayDataType )
//                      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case ArrayType(elementType2, _)]: seqInnerRowSeq: "      + seqInnerRowSeq )
                      
                        SchemaMerger.arrayTransformationFnc(
                            oRowOriginal,
                            seqInnerRowSeq,
                            oLocalArrayDataType,
                            oRecursivePathDefinition,
                            nLevel+1
                        )
      //                  Seq()
                      }//-- case ArrayType(elementType2, _)
                    
                    case StructType(fields2)          => {
//                      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case StructType(fields2)] " )
                      val oRowInner = mRowValue.asInstanceOf[Row] //if ( bValueExists ) mRowValue.getStruct( nIndex ) else null
                      
//                      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case StructType(fields2)]: oRowInner:  " + oRowInner )
                      SchemaMerger.recTransformationFnc( 
                          oRowOriginal,
                          oRowInner,
                          oRecursivePathDefinition.seqChilds,
                          oNewDataType.asInstanceOf[StructType],
                          false,
                          nLevel + 1
                          )
                    }
                    case _                           => {
//                      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case _ ] " )
//                      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case _ ]: oNewDataType: " + oNewDataType )
//                      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case _ ]: oRowIn: " + oRowIn )
  //                    val oRowInner = if ( bValueExists ) oRowIn.get( nIndex ) else null
  //                    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "[ case StructType(fields) ]: [case _ ]: oRowInner: " + oRowInner )
                      
                      //oLocalDataType
                      //org.apache.spark.sql.Row(oRowIn)
  //                    Seq( oRowInner )
                      mRowValue.asInstanceOf[Row].apply( nIndex )
                    }
                    
                  }//-- oLocalDataType match
                }
              }
              else
              {
                null
              }
            }
          }
          else
          {
            if (null == oRowIn)
            {
              null
            }
            else
            {
              oRowIn.apply( nIndex )
            }
          }
          
          nIndex = nIndex + 1
          
          mRes
        }).toSeq
        
        org.apache.spark.sql.Row.fromSeq(seqChildsValues)
      }//-- case StructType(fields)

      
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "Val oRowRes: " + oRowRes )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.recTransformationFnc( nLevel = " + nLevel + "): " + "END" )
    
    
    
    
    oRowRes
  }//-- def recTransformationFnc
  
  
//  def recTransformationFnc( 
//      oRowIn:                         org.apache.spark.sql.Row,
//      seqRecursivePathDefinitions:    Seq[ RecursivePathDefinition ],
//      oSchemaIn:                      org.apache.spark.sql.types.DataType,
//      bShouldFillSourceValues:        Boolean                             = false,
//      nLevel:                         Int                                 = 0
//      ): org.apache.spark.sql.Row
  
  def generateTransformationFnc( 
    seqRecursivePathDefinitions:    Seq[ RecursivePathDefinition ],
    oSchemaIn:                      org.apache.spark.sql.types.StructType,
    nLevel:                         Int       = 0
    ) = ( oRowIn: org.apache.spark.sql.Row ) => {
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.generateTransformationFnc( nLevel = " + nLevel + "): " + "START" )
    
    val oRes: org.apache.spark.sql.Row = SchemaMerger.recTransformationFnc( 
        oRowIn,
        oRowIn,
        seqRecursivePathDefinitions,
        oSchemaIn,
        false,
        nLevel
      ) 
    
    
      
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.generateTransformationFnc( nLevel = " + nLevel + "): " + "END" )
    oRes
    
  }//-- def generateTransformationFnc(...)
  
  
  def getSrcAndTgtPathFirstLevelNames(       oTransformationDefinition:      TransformationDefinition ): Seq[ String ] =
  {
    val seqPathDefinitions: Seq[ PathDefinition ] =  SchemaMerger.getSrcAndTgtPaths( oTransformationDefinition )
    
    val seqNames: Seq[String] = seqPathDefinitions.map( oPathDefinition => { oPathDefinition.seqPathTokens.head.strToken }).distinct
    
    seqNames
  }
  
  
    
  def getSrcAndTgtPaths(       oTransformationDefinition:      TransformationDefinition ): Seq[ PathDefinition ] =
  {
    
    val seqTargetPaths: Seq[PathDefinition] = oTransformationDefinition.seqColumnTransformationDefinitions.map( oColumnTransformationDefinition => {
      
      oColumnTransformationDefinition.oTargetPathDefinition
      
      
      
      
    }).filter( x => {x != null} ).toSeq
    
    
    
    val seqSourcePaths: Seq[PathDefinition] = oTransformationDefinition.seqColumnTransformationDefinitions.map( oColumnTransformationDefinition => {
      
      oColumnTransformationDefinition.seqSourceDefinition.map( oSourceDefinition => {
      if ( null == oSourceDefinition.oPath ) null else 
                          { 
                            val oPath = oSourceDefinition.oPath
                            
                            oPath
                          }
      }).toSeq
    }).flatten.filter( x => {x != null} ).toSeq
    
    
    
    seqTargetPaths ++ seqSourcePaths
    
  }
    
    
    
    
    
  def addSrcFetchingFunctions( 
      oTransformationDefinition:      TransformationDefinition,
      oSchemaIn:                      org.apache.spark.sql.types.StructType
  ): TransformationDefinition = {
    
    TransformationDefinition( oTransformationDefinition.seqColumnTransformationDefinitions.map( oColumnTransformationDefinition => {
      
      ColumnTransformationDefinition(
    oColumnTransformationDefinition.oTargetPathDefinition,
    oColumnTransformationDefinition.strFunctionName,
    oColumnTransformationDefinition.seqSourceDefinition.map( oSourceDefinition => {
//      SourceDefinition( oPath:  PathDefinition     = null,
//                             oConst: ConstantDefinition = null,
//                         var mValue: Any                = null)
      SourceDefinition( if ( null == oSourceDefinition.oPath ) null else 
                          { 
                            val oPath = oSourceDefinition.oPath
                            
                            PathDefinition(
                                oPath.seqPathTokens,
                                oPath.strSeparator,
                                RowUtils.fncGeneratorPathValue(oPath, oSchemaIn)
                            )
                            
                            
                          },
                        oSourceDefinition.oConst,
                        oSourceDefinition.mValue)
    }).toSeq,
    oColumnTransformationDefinition.bOverwriteTarget,
    oColumnTransformationDefinition.bDataParentExistenceNeeded
      )
      
      
    }).toSeq)
    
  }
  
  
  
  
  
  def generateTransformationUdf( 
      oTransformationDefinition:      TransformationDefinition,
      oSchemaIn:                      org.apache.spark.sql.types.StructType
  ): (org.apache.spark.sql.expressions.UserDefinedFunction, ( org.apache.spark.sql.Row ) => org.apache.spark.sql.Row, org.apache.spark.sql.types.DataType ) = {
    
    
//      def convertToRecursivePathDefinition( 
//      oTransformationDefinition:      TransformationDefinition,
//      oSchema:                        org.apache.spark.sql.types.StructType, 
//      nLevel:                     Int                      = 0
//  ): (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType)
    
    val oTransformationDefinitionExtended:      TransformationDefinition = SchemaMerger.addSrcFetchingFunctions( oTransformationDefinition, oSchemaIn )
    
    
    val oResPair: (Seq[(RecursivePathDefinition, org.apache.spark.sql.types.DataType)], org.apache.spark.sql.types.DataType) = SchemaMerger.convertToRecursivePathDefinition( oTransformationDefinitionExtended, oSchemaIn )
    
    
    val oResSchema = oResPair._2
    
    val seqRecursivePathDefinitions: Seq[RecursivePathDefinition] = oResPair._1.map( x => x._1 )
    
    
    val fncTrans: ( org.apache.spark.sql.Row ) => org.apache.spark.sql.Row = SchemaMerger.generateTransformationFnc( 
    seqRecursivePathDefinitions,
    oSchemaIn
    )
    
    val oResUdf = udf( fncTrans, oResSchema )
    
    ( oResUdf, fncTrans, oResSchema )
      
  }//-- def generateTransformationUdf(...)
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  case class ColumnsSelectionInfo( 
      seqColumnNamesToTransform: Seq[String],
      seqPairsToTransform: Seq[(String, RecursivePathDefinition )],
      seqFieldNames: Seq[ String ],
      seqOtherFieldNames: Seq[ String ],
      listUnTransformedColumns: List[ ( org.apache.spark.sql.Column, Int ) ],
      listUnTransformedNonKeyColumns: List[ ( org.apache.spark.sql.Column, Int ) ],
      listTransformedColumns:   List[ ( org.apache.spark.sql.Column, Int, org.apache.spark.sql.types.StructField, RecursivePathDefinition ) ]
  )
  
  
  
  def getColumnsSelectionInfoForStruct(
      df:                            org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      strStructColumnName:           String,
      seqRecursivePathDefinitions:   Seq[ RecursivePathDefinition ],
      listPrimaryKeyColumnNames:     List[ String ] = List()
      ): ColumnsSelectionInfo = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfoForStruct(): " + "START" )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfoForStruct(): " + "Param listPrimaryKeyColumnNames: " + listPrimaryKeyColumnNames )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfoForStruct(): " + "Param strStructColumnName: "       + strStructColumnName )
    
    
    
    val seqColumnNamesToTransform: Seq[String] = seqRecursivePathDefinitions.map( oRecursivePathDefinition => {
      
      oRecursivePathDefinition.oPathToken.strToken
      
    })
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfoForStruct(): " + "Val seqColumnNamesToTransform: " + seqColumnNamesToTransform )
    
    
    
    val seqPairsToTransform: Seq[(String, RecursivePathDefinition )] = seqRecursivePathDefinitions.map( oRecursivePathDefinition => {
      
      ( oRecursivePathDefinition.oPathToken.strToken, oRecursivePathDefinition)
      
    })
    
    val nFieldIndexOfStructColumn = df.schema.fieldIndex( strStructColumnName )
    
    
//    val seqFieldNames:            Seq[ String ] = df.schema.fieldNames.toSeq
    
    val oDataType = df.schema.fields( nFieldIndexOfStructColumn ).dataType
    
    val oStructType = oDataType.asInstanceOf[StructType]
    
    val seqFieldNames:            Seq[ String ] = oStructType.fieldNames.toSeq
    
    
    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfoForStruct(): " + "Val seqFieldNames: " + seqFieldNames )
    
    
    val seqOtherFieldNames: Seq[ String ]  = seqFieldNames.filter( strFieldName => { 
      !seqColumnNamesToTransform.contains(strFieldName) 
    })
    
    
    val listUnTransformedColumns:   List[ ( org.apache.spark.sql.Column, Int ) ] = seqOtherFieldNames.map( strFieldName => { 
      ( df.col(  "`" + strStructColumnName + "`.`" + strFieldName + "`" ).as( strStructColumnName + "." + strFieldName ), oStructType.fieldIndex( strFieldName ) ) 
    }).toList
    
    
    val listUnTransformedNonKeyColumns: List[ ( org.apache.spark.sql.Column, Int ) ] = seqOtherFieldNames.filter(x => {
      !listPrimaryKeyColumnNames.contains(x)
      })
      .map( strFieldName => {
        ( df.col( "`" + strStructColumnName + "`.`" + strFieldName + "`" ).as( strStructColumnName + "." + strFieldName ), oStructType.fieldIndex( strFieldName ) ) 
    }).toList
    
    
    val listTransformedColumns: List[ ( org.apache.spark.sql.Column, Int, org.apache.spark.sql.types.StructField, RecursivePathDefinition ) ] = seqPairsToTransform.map( oPair => {
      val strFieldName             = oPair._1
      val oRecursivePathDefinition = oPair._2
      (
          df.col(  "`" + strStructColumnName + "`.`" + strFieldName + "`" ).as( strStructColumnName + "." + strFieldName ), 
          oStructType.fieldIndex( strFieldName ), 
          oStructType.fields( oStructType.fieldIndex( strFieldName ) ), 
          oRecursivePathDefinition 
      )
    }).toList
    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfoForStruct(): " + "End" )
    
    
    
    ColumnsSelectionInfo( 
      seqColumnNamesToTransform,
      seqPairsToTransform,
      seqFieldNames,
      seqOtherFieldNames,
      listUnTransformedColumns,
      listUnTransformedNonKeyColumns,
      listTransformedColumns
    )
  }//-- def getColumnsSelectionInfo() ---
  
  
  
  def getColumnsSelectionInfo(
      df:                          org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      seqRecursivePathDefinitions: Seq[ RecursivePathDefinition ],
      listPrimaryKeyColumnNames:   List[ String ] = List()
      ): ColumnsSelectionInfo = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfo(): " + "START" )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfo(): " + "Param listPrimaryKeyColumnNames: " + listPrimaryKeyColumnNames )
    
    val seqColumnNamesToTransform: Seq[String] = seqRecursivePathDefinitions.map( oRecursivePathDefinition => {
      
      oRecursivePathDefinition.oPathToken.strToken
      
    })
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfo(): " + "Val seqColumnNamesToTransform: " + seqColumnNamesToTransform )
    
    
    val seqPairsToTransform: Seq[(String, RecursivePathDefinition )] = seqRecursivePathDefinitions.map( oRecursivePathDefinition => {
      
      ( oRecursivePathDefinition.oPathToken.strToken, oRecursivePathDefinition)
      
    })
    
    
    
    val seqFieldNames:            Seq[ String ] = df.schema.fieldNames.toSeq
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfo(): " + "Val seqFieldNames: " + seqFieldNames )
    
    
    val seqOtherFieldNames:       Seq[ String ] = seqFieldNames.filter( strFieldName => { !seqColumnNamesToTransform.contains(strFieldName) } )
    val listUnTransformedColumns: List[ ( org.apache.spark.sql.Column, Int ) ]                                         = seqOtherFieldNames.map( strFieldName => { ( df.col( "`" + strFieldName + "`" ), df.schema.fieldIndex( strFieldName ) ) } ).toList
    val listUnTransformedNonKeyColumns: List[ ( org.apache.spark.sql.Column, Int ) ]                                   = seqOtherFieldNames.filter(x => {!listPrimaryKeyColumnNames.contains(x)}).map( strFieldName => { ( df.col( "`" + strFieldName + "`" ), df.schema.fieldIndex( strFieldName ) ) } ).toList
    val listTransformedColumns:   List[ ( org.apache.spark.sql.Column, Int, org.apache.spark.sql.types.StructField, RecursivePathDefinition ) ] = seqPairsToTransform.map( oPair => { val strFieldName = oPair._1; val oRecursivePathDefinition = oPair._2; ( df.col( "`" + strFieldName + "`" ), df.schema.fieldIndex( strFieldName ), df.schema.fields( df.schema.fieldIndex( strFieldName ) ), oRecursivePathDefinition ) }).toList

    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.getColumnsSelectionInfo(): " + "End" )
    
    ColumnsSelectionInfo( 
      seqColumnNamesToTransform,
      seqPairsToTransform,
      seqFieldNames,
      seqOtherFieldNames,
      listUnTransformedColumns,
      listUnTransformedNonKeyColumns,
      listTransformedColumns
    )
  }//-- def getColumnsSelectionInfo() ---
  
  

  
  
  
  
  def createSelectColumns(
      df:                          org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      seqRecursivePathDefinitions: Seq[ RecursivePathDefinition ]
      ): List[org.apache.spark.sql.Column] = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.createSelectColumns(): " + "START" )
    
    
    val oColumnsSelectionInfo: ColumnsSelectionInfo = getColumnsSelectionInfo( df, seqRecursivePathDefinitions )
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.createSelectColumns(): " + "Var oColumnsSelectionInfo.seqFieldNames.size: "      + oColumnsSelectionInfo.seqFieldNames.size )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.createSelectColumns(): " + "Var oColumnsSelectionInfo.seqOtherFieldNames.size: " + oColumnsSelectionInfo.seqOtherFieldNames.size )
    
    val listTransformedColumns: List[ ( org.apache.spark.sql.Column, Int ) ] = SchemaMerger.transformColumns( df, oColumnsSelectionInfo.listTransformedColumns )
    
    (oColumnsSelectionInfo.listUnTransformedColumns ++ listTransformedColumns).sortBy(_._2).map( oParir => { oParir._1 } ).toList
    
  }//-- def createSelectColumns(...) 
  
  
  
  def transformColumns( 
      df:                        org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
      listTransformedColumns:    List[ ( org.apache.spark.sql.Column, Int, org.apache.spark.sql.types.StructField, RecursivePathDefinition ) ],
      listPrimaryKeyColumnNames: List[ String ] = List( "nm_click_id" ),
      nArrayLevel:               Int            = 0
      ): List[ ( org.apache.spark.sql.Column, Int ) ] = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "START" )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Param listTransformedColumns: "    + listTransformedColumns )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Param listPrimaryKeyColumnNames: " + listPrimaryKeyColumnNames )
    
    
    val listRes: List[ ( org.apache.spark.sql.Column, Int ) ] = listTransformedColumns.map( oTuple => {
      
      val oColumn                   = oTuple._1
      val nFieldIndex               = oTuple._2
      val oInputField               = oTuple._3
      val oRecursivePathDefinition  = oTuple._4
      
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var oColumn: "                  + oColumn )
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var nFieldIndex: "              + nFieldIndex )
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var oInputField: "              + oInputField )
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var oRecursivePathDefinition: " + oRecursivePathDefinition )
      
      
      
      val strColumnName             = oInputField.name
      val oDataType                 = oInputField.dataType
      
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var strColumnName: "            + strColumnName )
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var oDataType: "                + oDataType )
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var oDataType.typeName: "       + oDataType.typeName )
      
      
      
      val seqChildRecursivePathDefinition: Seq[ RecursivePathDefinition ] =   oRecursivePathDefinition.seqChilds
      val bIsLeaf:                         Boolean                        = ( oRecursivePathDefinition.oColumnTransformationDefinition != null )
      
      
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var seqChildRecursivePathDefinition: "   + seqChildRecursivePathDefinition )
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var bIsLeaf: "                           + bIsLeaf )
      
      
      val listPrimaryKeyColumns: List[ org.apache.spark.sql.Column ] = listPrimaryKeyColumnNames.map( strColumnName => df.col("`" + strColumnName + "`") ).toList
      
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Var listPrimaryKeyColumns: "             + listPrimaryKeyColumns )
      
      
      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[Prolog]: Var df.show: "  )
      df.show

      
      oDataType match {
        
        case ArrayType(elementType, _)                     => {
          
          val localDf = df.select( (listPrimaryKeyColumns :+  posexplode_outer(oColumn)): _* )
                                  .withColumnRenamed("pos", "id_level_" + nArrayLevel)
                                  .withColumnRenamed("col", strColumnName)
          ;
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[ArrayType]: Var localDf.show: "  )
          localDf.show
          
          val oColumnValue                  = localDf.col( strColumnName )
          val nLocalFieldIndex              = localDf.schema.fieldIndex( strColumnName )
          val oLocalField                   = localDf.schema.fields( localDf.schema.fieldIndex( strColumnName ) )
          
          val oLocalRecursivePathDefinition = if ( bIsLeaf )
          {
            oRecursivePathDefinition//oRecursivePathDefinition
          }
          else
          {
            oRecursivePathDefinition//seqChildRecursivePathDefinition.head
          }
          
          
          val listLocalTransformedColumns:    List[ ( org.apache.spark.sql.Column, Int, org.apache.spark.sql.types.StructField, RecursivePathDefinition ) ] = 
            List(
                  ( oColumnValue, nLocalFieldIndex,  oLocalField , oLocalRecursivePathDefinition )
                )
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[ArrayType]: Var listLocalTransformedColumns: "             + listLocalTransformedColumns )
          
          
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[ArrayType]: Var localDf.show: "  )
          localDf.show
          
          val listArrayPair = SchemaMerger.transformColumns( 
              localDf,
              listLocalTransformedColumns,
              listPrimaryKeyColumnNames ++ List( "id_level_" + nArrayLevel ),
              nArrayLevel+1
          )
          
          
          //val arrayDf = localDf.groupBy( listPrimaryKeyColumns: _* ).agg( collect_list( localDf.col("value_col") ).as(strColumnName) )
          var oArrayResDf = localDf
          
          listArrayPair.foreach( oArrayPair => {
            
            println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[ArrayType]: [In loop] Var oArrayResDf.show: "  )
            oArrayResDf.show

            
            oArrayResDf = oArrayResDf.withColumn( "___res___", oArrayPair._1 )
//            oArrayResDf = oArrayResDf.withColumn( "___res___" + oArrayPair._2, oArrayPair._1 )
            
          })
          
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[ArrayType]: Var oArrayResDf.show: "  )
          oArrayResDf.show
          
          
          
          val arrayDf = oArrayResDf.groupBy( listPrimaryKeyColumns: _* ).agg( collect_list( oArrayResDf.col("___res___") ).as(strColumnName) )
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[ArrayType]: Var arrayDf.show: "  )
          arrayDf.show
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[ArrayType]: Var arrayDf.schema: " + arrayDf.schema )
          
          
          SchemaMerger.m_oGlobalArrayDF = arrayDf
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[ArrayType]: Var localDf.select( arrayDf.col(`" + strColumnName + "`) ).show: "  )
          
          localDf.select( arrayDf.col("`" + strColumnName + "`") ).show
          
          
          ( arrayDf.col("`" + strColumnName + "`"), nFieldIndex )
          //array( strColumnName )
          
        
        }
        case StructType(fields)                            => {
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Found a nested struct type: " + oDataType.typeName )
          
          val oDataStructType = oDataType.asInstanceOf[StructType]
          
          val listColumnNamesToTransform: List[String] = seqChildRecursivePathDefinition.map( oStructRecursivePathDefinition => {
      
            oStructRecursivePathDefinition.oPathToken.strToken
            
          }).toList
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var listColumnNamesToTransform: " + listColumnNamesToTransform )
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var df.show: " )
          df.show
          
          
          val listStructColumns = /*listColumnNamesToTransform */ oDataStructType.fieldNames.map( x => { 
            
            val strCurrentColName = "`" + strColumnName + "`.`" + x + "`"
            
            println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var strCurrentColName: " + strCurrentColName )
            
            df.col( strCurrentColName )
            
            
            })
          
          val listKeyColumns    = listPrimaryKeyColumnNames.map( x => { 
            
            val strCurrentColName = "`" + x + "`"
            
            println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var strCurrentColName: " + strCurrentColName )
            
            df.col( strCurrentColName ) 
            })
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var listStructColumns: " + listStructColumns )
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var listKeyColumns: "    + listKeyColumns )
          
          

//          val structDf = df.select( ( listKeyColumns ++ listStructColumns ): _* )
//          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "structDf.show: " )
//          structDf.show
          
          
          val oColumnsSelectionInfo: ColumnsSelectionInfo = getColumnsSelectionInfoForStruct( df, strColumnName, seqChildRecursivePathDefinition, listPrimaryKeyColumnNames )
    
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var oColumnsSelectionInfo.seqFieldNames.size: "      + oColumnsSelectionInfo.seqFieldNames.size )
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var oColumnsSelectionInfo.seqOtherFieldNames.size: " + oColumnsSelectionInfo.seqOtherFieldNames.size )
          
          val listStructRes: List[ ( org.apache.spark.sql.Column, Int ) ] = SchemaMerger.transformColumns( 
              df, 
              oColumnsSelectionInfo.listTransformedColumns,
              listPrimaryKeyColumnNames,
              nArrayLevel+1 )
              
              
//              strColumnName
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: [After inner transformColumns ] Var strColumnName: " + strColumnName )
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: [After inner transformColumns ] Var df.show: " )
          df.show(false)
              
              
          SchemaMerger.m_oGlobalStructDF = df
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: Var listStructRes: " + listStructRes )
          
          listStructRes.foreach( oPair => {
              
            val oCurrCol = oPair._1
            SchemaMerger.m_oCurrentCol = oCurrCol
            println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "[StructType]: [listStructRes.foreach] Var df.select( oCurrCol ).show: " )
            println( oCurrCol )
            df.select( oCurrCol ).show
          
          })
          
          
          
          val oResColumn = struct((oColumnsSelectionInfo.listUnTransformedNonKeyColumns ++ listStructRes).sortBy(_._2).map( oParir => { oParir._1 } ).toList:_* ).as(strColumnName)
          
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns(): " + "[StructType]: Var df.select( oResColumn ).show: " )
          df.select( oResColumn ).show
          
          
          ( oResColumn, nFieldIndex )
        }
        case MapType(keyType, valueType, containsNull)     => {
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Error:  Found a nested (unsupported) map type: " + oDataType.typeName )
          
          
          ( oColumn, nFieldIndex )
        }
        case _                                             => {
          println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Not a nested type: " + oDataType.typeName )
          
          oDataType match {
            
            case StringType                                    => {
              
              println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "( Inner ) String type found: " + oDataType.typeName )
              
              
              
              
              ( array(oColumn).as(strColumnName), nFieldIndex )
            }
            case IntegerType | ByteType | LongType | ShortType => {
              
              ( oColumn, nFieldIndex )
            }
            case DoubleType | LongType                         => {
              
              ( oColumn, nFieldIndex )
            }
            case BooleanType                                   => {
              
              
              ( oColumn, nFieldIndex )
            }
            
            case _                                             => {
              
              
              ( oColumn, nFieldIndex )
            }
            
            
          }//-- oDataType match [inner]
          
        }//-- case _ [top level]
        
      }//-- oDataType match  [top level]
      
    })
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "Val listRes: " + listRes )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformColumns( nArrayLevel = " + nArrayLevel + " ): " + "END" )
    
    listRes
  }//-- def transformColumns(...)
  
  
  
  
  
    
//  def transformUsingUdf( 
//      df:                        org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
//      listTransformedColumns:    List[ ( org.apache.spark.sql.Column, Int, org.apache.spark.sql.types.StructField, RecursivePathDefinition ) ],
//      listPrimaryKeyColumnNames: List[ String ] = List( "nm_click_id" ),
//      nArrayLevel:               Int            = 0
//      ): List[ ( org.apache.spark.sql.Column, Int ) ] = {
//    
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "START" )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Param listTransformedColumns: "    + listTransformedColumns )
//    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Param listPrimaryKeyColumnNames: " + listPrimaryKeyColumnNames )
//    
//    
//    val listRes: List[ ( org.apache.spark.sql.Column, Int ) ] = listTransformedColumns.map( oTuple => {
//      
//      val oColumn                                                         = oTuple._1
//      val nFieldIndex                                                     = oTuple._2
//      val oInputField                                                     = oTuple._3
//      val oRecursivePathDefinition                                        = oTuple._4
//      val strColumnName                                                   = oInputField.name
//      val oDataType                                                       = oInputField.dataType
//      val seqChildRecursivePathDefinition: Seq[ RecursivePathDefinition ] =   oRecursivePathDefinition.seqChilds
//      val bIsLeaf:                         Boolean                        = ( oRecursivePathDefinition.oColumnTransformationDefinition != null )
//      
//      
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var oColumn: "                  + oColumn )
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var nFieldIndex: "              + nFieldIndex )
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var oInputField: "              + oInputField )
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var oRecursivePathDefinition: " + oRecursivePathDefinition )
//      
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var strColumnName: "            + strColumnName )
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var oDataType: "                + oDataType )
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var oDataType.typeName: "       + oDataType.typeName )
//      
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var seqChildRecursivePathDefinition: "   + seqChildRecursivePathDefinition )
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var bIsLeaf: "                           + bIsLeaf )
//      
//      
//      
//      
//      
//      
//      
//      val listPrimaryKeyColumns: List[ org.apache.spark.sql.Column ] = listPrimaryKeyColumnNames.map( strColumnName => df.col("`" + strColumnName + "`") ).toList
//      
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "Var listPrimaryKeyColumns: "             + listPrimaryKeyColumns )
//      
//      
//      println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformUsingUdf( nArrayLevel = " + nArrayLevel + " ): " + "[Prolog]: Var df.show: "  )
//      df.show
//      
//      
//      
//      
//      
//      
//      
//      
//
//    })
//  }
  
  
  
  
  
  def transformRow( 
      seqRecursivePathDefinitions: Seq[ RecursivePathDefinition ],
      oRowTarget:                  org.apache.spark.sql.Row,
      oRowRoot:                    org.apache.spark.sql.Row,
      nRecLevel:                   Int                       = 0
      
      
  ): org.apache.spark.sql.Row = {
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "START" )
    
    val seqColumnNamesToTransform: Seq[String] = seqRecursivePathDefinitions.map( oRecursivePathDefinition => {
      
      oRecursivePathDefinition.oPathToken.strToken
      
    })
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var seqColumnNamesToTransform: " + seqColumnNamesToTransform )
    
    val seqFieldNames: Seq[ String ] = oRowTarget.schema.fieldNames.toSeq
    
    val seqOtherFieldNames = seqFieldNames.filter( strFieldName => { !seqColumnNamesToTransform.contains(strFieldName) } )
    
    
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var seqFieldNames.size: "      + seqFieldNames.size )
    println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var seqOtherFieldNames.size: " + seqOtherFieldNames.size )
    
    
    
    val seqRowValuesOther = seqOtherFieldNames.map( strFieldName => { ( oRowTarget.fieldIndex( strFieldName ), oRowTarget.get( oRowTarget.fieldIndex( strFieldName ) ) ) })
    
    
    
    
    val seqRowValues = seqRecursivePathDefinitions.map( oRecursivePathDefinition => {
      
                               val strFieldNameToTransform:          String                         = oRecursivePathDefinition.oPathToken.strToken
                               val oColumnTransformationDefinition:  ColumnTransformationDefinition = oRecursivePathDefinition.oColumnTransformationDefinition
                               
                               
                               val nFieldIndex                            = oRowTarget.fieldIndex(    strFieldNameToTransform )
                               val oField                                 = oRowTarget.schema.fields( nFieldIndex             )
                               val bIsNull                                = oRowTarget.isNullAt(      nFieldIndex             )
                               val bIsEmptyColumnTransformationDefinition = ( null == oColumnTransformationDefinition )
                               
                               println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var nFieldIndex: "                   + nFieldIndex )
                               println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var oField.name: "                   + oField.name )
                               println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var oField.dataType.catalogString: " + oField.dataType.catalogString )
                               println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var oField.dataType.typeName: "      + oField.dataType.typeName )
                               
                               
                               if ( bIsNull && bIsEmptyColumnTransformationDefinition )
                               {
                                 // <null> && not leaf
                                 println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "ERROR: Row-value is <null> and it is not a leaf. Ignoring nested ColumnTransformationDefinitions. " + oRecursivePathDefinition )
                                 
                               }//-- if ( bIsNull && bIsEmptyColumnTransformationDefinition )
                               else
                               {
                                 
                                 if ( bIsNull )
                                 {
                                   // <null> && leaf
                                   println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Row-value is <null>, but it is a leaf. " )
                                 }//-- if ( bIsNull )
                                 else
                                 {
                                   val oDataType = oField.dataType
                                   
                                   if ( bIsEmptyColumnTransformationDefinition )
                                   {
                                     // Non-<null> && not leaf
                                     println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Row-value is Non-<null>, and it is NOT a leaf. " )
                                     
//                                     oDataType   match {
//                                        case ArrayType(elementType, _)                     => "list(" + dataTypeName(elementType) + ")"
//                                        case StructType(fields)                            => "struct(" + fields.map { subField => dataTypeName(subField.dataType) }.mkString(",") + ")"
//                                        case MapType(keyType, valueType, containsNull)     => s"map($keyType,$valueType)"
//                                        case _                                             => oDataType.typeName
//                                     }
                                   }
                                   else
                                   {
                                     // Non-<null> && leaf
                                     println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Row-value is Non-<null>, and it is a leaf. " )
                                     
//                                     oDataType   match {
//                                        case StringType                                    => "string"
//                                        case IntegerType | ByteType | LongType | ShortType => "int"
//                                        case DoubleType | LongType                         => "float"
//                                        case BooleanType                                   => "bool"
//                                        case DateType                                      => "date"
//                                        case TimestampType                                 => "time"
//                                        case BinaryType                                    => "binary"
//                                        case NullType                                      => "null"
//                                        case _                                             => oDataType.typeName
//                                     }
                                     
                                   }
                                 }//-- else ( bIsNull )
                                 
                                 
                                 
//                                 if ( oField.dataType.typeName.equals("array") )
//                                 {
//                                   
//                                   // Vector(36, 59, 285, 308, 400, 423, 524, 536, 745, 781)
//                                   val seqContent = oRowTarget.getSeq( nFieldIndex )
//                                   
//                                   println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var seqContent: " + seqContent )
//                                   
//                                   
//                                 }//-- if ( oField.dataType.typeName.equals("array") )
                                 
                               }//-- else ( bIsNull && bIsEmptyColumnTransformationDefinition )
                               
      
    })
    
    
//    val seqRowValues      = seqColumnNamesToTransform.map( strFieldNameToTransform => {
//      
//                               val nFieldIndex = oRowTarget.fieldIndex(    strFieldNameToTransform )
//                               val oField      = oRowTarget.schema.fields( nFieldIndex )
//                               val bIsNull     = oRowTarget.isNullAt(      nFieldIndex )
//                               
//                               
//                               println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var nFieldIndex: "                   + nFieldIndex )
//                               println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var oField.name: "                   + oField.name )
//                               println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var oField.dataType.catalogString: " + oField.dataType.catalogString )
//                               println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var oField.dataType.typeName: "      + oField.dataType.typeName )
//                               
//                               
//                               if ( oField.dataType.typeName.equals("array") )
//                               {
//                                 if ( bIsNull )
//                                 {
//                                   
//                                 }
//                                 else
//                                 {
//                                   // Vector(36, 59, 285, 308, 400, 423, 524, 536, 745, 781)
//                                   val seqContent = oRowTarget.getSeq( nFieldIndex )
//                                   
//                                   println( "de.mindlab.spark_schema_transformer.row_iterator.SchemaMerger.transformRow( nRecLevel = " + nRecLevel + "): " + "Var seqContent: " + seqContent )
//                                 }
//                                 
//                                 
//                                 
//                               }
//                               
//                            })
    
    oRowRoot
    
  }//-- transformRow( ... ) ...
  
  
}