package de.mindlab.spark_schema_transformer.row_iterator.functions

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import scala.collection.Seq


object TransformationFunctions {
  
  val fncCollect       = ( seqSrc: Seq[Any], tgt: Any )     => { Seq(tgt)             }
  val typeFncCollect   = ( seqSrc: Seq[Any], x:   DataType) => { ArrayType( x, true ) }
  val strKeyFncCollect = "collect"
  
  val fncAddStringStructField  = ( seqSrc: Seq[Any], tgt: Any ) => {
    
    val strFieldName = seqSrc(0)
    val mValue       = seqSrc(1)
    
    val oTgtRow      = tgt.asInstanceOf[Row]
    
    val seqRowIn: Seq[Any] = oTgtRow.toSeq
    
    
    val seqRowOut: Seq[Any] = seqRowIn :+ mValue
    
    Row.fromSeq( seqRowOut )
    
    
  }//-- val fncAddStringStructField
  
  
  val typeFncAddStringStructField   = (seqSrc: Seq[Any], x: DataType) => {
      val strFieldName: String = seqSrc(0).asInstanceOf[String]
      val typeRes: StructType = x.asInstanceOf[StructType].add(strFieldName, "StringType", false)
      typeRes
    }//-- val typeFncAddStringStructField
  val strKeyFncAddStringStructField = "addStringStructField"
  //--------------------------------------------------------------------------------------------------------------------
    val fncAddStringStructFieldInArray  = ( seqSrc: Seq[Any], tgt: Any ) => {
    
    val strFieldName = seqSrc(0)
    val mValue       = if ( seqSrc(1) == null ) "" else seqSrc(1)
    
    
    val seqTgt = tgt.asInstanceOf[Seq[Row]].map( oTgt => {
      
      val oTgtRow      = oTgt.asInstanceOf[Row]
    
      val seqRowIn: Seq[Any] = oTgtRow.toSeq
      
      
      val seqRowOut: Seq[Any] = seqRowIn :+ mValue
      
      Row.fromSeq( seqRowOut )
      
    } )
    
    
    seqTgt
    
  }//-- val fncAddStringStructField
  
  
  val typeFncAddStringStructFieldInArray   = (seqSrc: Seq[Any], x: DataType) => {
    
//      println( "de.mindlab.schemamerger.TransformationFunctions.typeFncAddStringStructFieldInArray(): " + "START" )
//      println( "de.mindlab.schemamerger.TransformationFunctions.typeFncAddStringStructFieldInArray(): " + "Param seqSrc: " + seqSrc )
      
      
      val strFieldName: String = seqSrc(0).asInstanceOf[String]
//      println( "de.mindlab.schemamerger.TransformationFunctions.typeFncAddStringStructFieldInArray(): " + "Val strFieldName: " + strFieldName )
    
      val typeRes: StructType = x.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].add(strFieldName, StringType, false)
      val oRes = ArrayType(typeRes)
      
//      println( "de.mindlab.schemamerger.TransformationFunctions.typeFncAddStringStructFieldInArray(): " + "Val oRes: " + oRes )
      oRes
    }//-- val typeFncAddStringStructField
  val strKeyFncAddStringStructFieldInArray = "addStringStructFieldInArray"
  //--------------------------------------------------------------------------------------------------------------------
  
  // (Seq[Any], Any) => Seq[Any]
  
  val m_mapFunctionPairs: Map[String,( ((Seq[Any], Any) => Any) , (( Seq[Any],  DataType) => DataType) )] = Map(
      
  (strKeyFncCollect                     -> ( fncCollect,                     typeFncCollect                     ) ),
  (strKeyFncAddStringStructField        -> ( fncAddStringStructField,        typeFncAddStringStructField        ) ),
  (strKeyFncAddStringStructFieldInArray -> ( fncAddStringStructFieldInArray, typeFncAddStringStructFieldInArray ) )
  
  
  
  )//-- 
  
  
  
  def getPair( strKey: String ): ( ((Seq[Any], Any) => Any) , ((  Seq[Any], DataType) => DataType)) = {
    
    TransformationFunctions.m_mapFunctionPairs(strKey)
    
  }
  
  
  def getFnc( strKey: String ): ((Seq[Any], Any) => Any) = {
    
    
    
    TransformationFunctions.m_mapFunctionPairs(strKey)._1
    
  }
  
  
  def getTransformation( strKey: String ):  ((  Seq[Any], DataType) => DataType) = {
    
//    println( "de.mindlab.schemamerger.TransformationFunctions.getTransformation(): " + "START" )
//    println( "de.mindlab.schemamerger.TransformationFunctions.getTransformation(): " + "Param strKey: " + strKey )
//    println( "de.mindlab.schemamerger.TransformationFunctions.getTransformation(): " + "Var  TransformationFunctions.m_mapFunctionPairs: " + TransformationFunctions.m_mapFunctionPairs )
    
    
    
    TransformationFunctions.m_mapFunctionPairs(strKey)._2
    
  }
  
  
}