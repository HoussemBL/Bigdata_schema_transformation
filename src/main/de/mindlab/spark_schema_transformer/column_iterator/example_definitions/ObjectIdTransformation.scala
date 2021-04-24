package de.mindlab.spark_schema_transformer.column_iterator.example_definitions

import de.mindlab.spark_schema_transformer.row_iterator._
import de.mindlab.spark_schema_transformer.row_iterator.definitions._
import de.mindlab.spark_schema_transformer.column_iterator.utils._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.DataType
import scala.collection.mutable._
import scala.collection.mutable.PriorityQueue
import org.apache.spark.sql.types.DataType

//we define in a static way all  "T => ARRAY[T]"
// Object ID propagation is performed automatically
object ObjectIdTransformation {

  // T => ARRAY[T]:
//Transformations to do:
//nm_process_Processes.list.element.Error
//nm_process_Processes.list.element.Help
//nm_assisted_Processes.list.element.Error
//nm_assisted_Processes.list.element.Help
//nm_backwarded_process_Processes.list.element.Error
//nm_backwarded_process_Processes.list.element.Help
//prv_nm_process_Processes.list.element.Error
//prv_nm_process_Processes.list.element.Help
  val pTRoot = PathToken("root")
  val pT8 = PathToken("nm_process_Processes")
  val pT9 = PathToken("Error")
  val pT10 = PathToken("Help")
  val pT11 = PathToken("nm_assisted_Processes")
  val pT12 = PathToken("nm_backwarded_process_Processes")
  val pT13 = PathToken("prv_nm_process_Processes")

  val seq6 = Seq[PathToken](pTRoot, pT8, pT9)
  val path6 = PathDefinition(seq6)

  val seq7 = Seq[PathToken](pTRoot, pT8, pT10)
  val path7 = PathDefinition(seq7)

  val seq8 = Seq[PathToken](pTRoot, pT11, pT9)
  val path8 = PathDefinition(seq8)

  val seq9 = Seq[PathToken](pTRoot, pT11, pT10)
  val path9 = PathDefinition(seq9)

  val seq10 = Seq[PathToken](pTRoot, pT12, pT9)
  val path10 = PathDefinition(seq10)

  val seq11 = Seq[PathToken](pTRoot, pT12, pT10)
  val path11 = PathDefinition(seq11)

  val seq12 = Seq[PathToken](pTRoot, pT13, pT9)
  val path12 = PathDefinition(seq12)

  val seq13 = Seq[PathToken](pTRoot, pT13, pT10)
  val path13 = PathDefinition(seq13)

  val src6 = SourceDefinition(path6)
  val src7 = SourceDefinition(path7)
  val src8 = SourceDefinition(path8)
  val src9 = SourceDefinition(path9)
  val src10 = SourceDefinition(path10)
  val src11 = SourceDefinition(path11)
  val src12 = SourceDefinition(path12)
  val src13 = SourceDefinition(path13)

  val seqSrc6 = Seq[SourceDefinition](src6)
  val colTrans6 = ColumnTransformationDefinition(path6, "NameAsArray", seqSrc6, true)

  val seqSrc7 = Seq[SourceDefinition](src7)
  val colTrans7 = ColumnTransformationDefinition(path7, "NameAsArray", seqSrc7, true)

  val seqSrc8 = Seq[SourceDefinition](src8)
  val colTrans8 = ColumnTransformationDefinition(path8, "NameAsArray", seqSrc8, true)

  val seqSrc9 = Seq[SourceDefinition](src9)
  val colTrans9 = ColumnTransformationDefinition(path9, "NameAsArray", seqSrc9, true)

  val seqSrc10 = Seq[SourceDefinition](src10)
  val colTrans10 = ColumnTransformationDefinition(path10, "NameAsArray", seqSrc10, true)

  val seqSrc11 = Seq[SourceDefinition](src11)
  val colTrans11 = ColumnTransformationDefinition(path11, "NameAsArray", seqSrc11, true)

  val seqSrc12 = Seq[SourceDefinition](src12)
  val colTrans12 = ColumnTransformationDefinition(path12, "NameAsArray", seqSrc12, true)

  val seqSrc13 = Seq[SourceDefinition](src13)
  val colTrans13 = ColumnTransformationDefinition(path13, "NameAsArray", seqSrc13, true)

  val seqTrs1 = ListBuffer[ColumnTransformationDefinition](colTrans6, colTrans7, colTrans8, colTrans9, colTrans10,
    colTrans11, colTrans12, colTrans13)

  def getFullTransformation( fileToParse:String ): ListBuffer[ColumnTransformationDefinition] =
    { 
      val parser = TextInputParser(fileToParse)
      var seqTrs = TextInputParser.parser(parser)
      seqTrs 
    }


  def getStringToArrayTransformation(): ListBuffer[ColumnTransformationDefinition] =
    { 
   
      seqTrs1 
    }



  // if we have transformation a.b and a.b.c, thiertransformation will bve unified and done at Once
  def organizeTransformation(objInferredTypes: List[List[(String, DataType)]],operation:String): scala.collection.immutable.Map[String, PriorityQueue[InputTransformation]] =
    {

      var result = ListBuffer[AggregateTransformation]()
      var queueRes = new PriorityQueue[InputTransformation]()(Ordering.by(inputTransOrder))
      var dictionaryTransformation = Map[String, PriorityQueue[InputTransformation]]()

      objInferredTypes.foreach(listTrans =>
        {
          var a = listTrans(0)._1
          var key = a+"$"+operation
          var path=""
          
          var types=ListBuffer[DataType]()
             listTrans.map(f=> types+=f._2)
          if (a.contains(",")) { key = a.split(",")(0)  
              path=a.replaceAll(",", ("\\."))}
         else{ listTrans.map(f=> path=path+f._1+".").dropRight(1)}
          
          var obj = InputTransformation(path.split("\\.").size, path, types.toList)
          if (dictionaryTransformation.contains(key)) {
            var xx = dictionaryTransformation.get(key).get
            xx += obj
            dictionaryTransformation(key) = xx

          } else {

            var queueIntermidiate = new PriorityQueue[InputTransformation]()(Ordering.by(inputTransOrder))
            queueIntermidiate.+=(obj)
            dictionaryTransformation += (key -> queueIntermidiate)
          }

        })


         dictionaryTransformation
//convert the dictionary to immutable map
   var res=dictionaryTransformation.map(kv => (kv._1,kv._2)).toMap
         res
    }



  def inputTransOrder(d: InputTransformation) = d.depthlevel

}