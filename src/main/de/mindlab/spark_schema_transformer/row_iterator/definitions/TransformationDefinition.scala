package de.mindlab.spark_schema_transformer.row_iterator.definitions

import scala.collection.Seq
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions._
import scala.util.control.Breaks._
import org.apache.spark.sql.functions._
import scala.collection.mutable.PriorityQueue
import java.util.HashMap

case class TransformationDefinition(
  seqColumnTransformationDefinitions: Seq[ColumnTransformationDefinition])

object TransformationDefinition extends Serializable {
  var newColsCounter = 0
  var it_list = List[String]("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "p", "o", "v", "u", "x", "y", "z")
/******************************************* Row-based Approach***************************************************/

  def getLeafAndBranch(
    oTransformationDefinition: TransformationDefinition): (ColumnTransformationDefinition, TransformationDefinition) = {

    var oLeaf: ColumnTransformationDefinition = null

    var oBranch: TransformationDefinition = null

    var seqChildren: Seq[ColumnTransformationDefinition] = Seq()

    oTransformationDefinition.seqColumnTransformationDefinitions.foreach(oColumnTransformationDefinition => {

      if (oColumnTransformationDefinition.oTargetPathDefinition.seqPathTokens.size == 0) {
        oLeaf = oColumnTransformationDefinition
      } else {
        seqChildren = seqChildren :+ oColumnTransformationDefinition
      }

    })

    if (seqChildren.size > 0) {
      oBranch = TransformationDefinition(seqChildren)
    }

    (oLeaf, oBranch)
  }

  def getFirstPathTokens(
    oTransformationDefinition: TransformationDefinition): Seq[(PathToken, TransformationDefinition)] = {

    //    var seqRes: Seq[(PathToken, TransformationDefinition)] = Seq()

    var seqLocalPaths: Seq[(String, PathToken, ColumnTransformationDefinition)] = Seq()

    oTransformationDefinition.seqColumnTransformationDefinitions.map(oColumnTransformationDefinition => {

      val oFirstPathToken = oColumnTransformationDefinition.oTargetPathDefinition.seqPathTokens.head

      seqLocalPaths = seqLocalPaths :+ (oFirstPathToken.strToken, oFirstPathToken, oColumnTransformationDefinition)

    })

    val mapGroupedPaths: Map[String, Seq[(String, PathToken, ColumnTransformationDefinition)]] = seqLocalPaths.groupBy(x => x._1)

    val seqRes: Seq[(PathToken, TransformationDefinition)] = mapGroupedPaths.map(oMapElement => {

      val strCurrentPathToken: String = oMapElement._1
      val seqColumnTransformationDefinitionInfo: Seq[(String, PathToken, ColumnTransformationDefinition)] = oMapElement._2

      val oPathToken: PathToken = seqColumnTransformationDefinitionInfo.head._2

      val seqColumnTransformationDefinitions: Seq[ColumnTransformationDefinition] = seqColumnTransformationDefinitionInfo.map(x => {

        val oOldColumnTransformationDefinition: ColumnTransformationDefinition = x._3

        val oOldTargetPathDefinition: PathDefinition = oOldColumnTransformationDefinition.oTargetPathDefinition

        val oNewTargetPathDefinition: PathDefinition = PathDefinition(oOldTargetPathDefinition.seqPathTokens.tail, oOldTargetPathDefinition.strSeparator)

        val oNewColumnTransformationDefinition: ColumnTransformationDefinition = ColumnTransformationDefinition(
          oNewTargetPathDefinition,
          oOldColumnTransformationDefinition.strFunctionName,
          oOldColumnTransformationDefinition.seqSourceDefinition,
          oOldColumnTransformationDefinition.bOverwriteTarget)

        oNewColumnTransformationDefinition

      })

      val oCurrentTransformationDefinition: TransformationDefinition = TransformationDefinition(seqColumnTransformationDefinitions)

      (oPathToken, oCurrentTransformationDefinition)
    }).toSeq

    seqRes
  } // def getFirstPathTokens

/******************************************* Column-based Approach***************************************************/

  
    // the global function that will do the transformation following an aggregated way
  //all related path are clustred and transformed in one transformation
  //no execution , just collect necessary transformations
  def generateExecutionPlan(df: DataFrame, aggTrans: Map[String, PriorityQueue[InputTransformation]]): List[(String,String)] = {
     var transTODO=ListBuffer[(String, String)]()

    aggTrans.foreach(f => {
      var transformationClusterPlan = transformClusterPlan(df, f)
    transTODO++=transformationClusterPlan
     
    })
    return transTODO.toList
  }
  
  
  
  //apply an execution plan of transformation
  def applyExecutionPlan(df: DataFrame, plan: List[(String,String)]): DataFrame = {
   var df_res = df
    plan/*(.par)*/.foreach(f => {
   df_res = executeOneTransformation(f._1.split("\\$")(0), f, df_res)    
    })
    return df_res
  }
  
  
     //execute a set of related transformations at once
  def executeOneTransformation(rootChange: String, clusterTransOps: (String, String), df: DataFrame): DataFrame = {
   // var df_res = df
    var highOrderQueryDefinition = HigherOrderFunctions(List[(String, String)](clusterTransOps))
    var df2 = df/*_res*/.withColumn("newCol", expr(highOrderQueryDefinition))
    df2 = df2.drop(df2.col(rootChange)).withColumn(rootChange.replaceAll("`", ""), df2.col("newCol"))
    return df2.drop("newCol")
  }
  
  

  
  
  // the global function that will do the transformation following an aggregated way
  //all related path are clustered and transformed in one transformation
  def TransformDfBatchAggregate(df: DataFrame, aggTrans: Map[String, PriorityQueue[InputTransformation]]): DataFrame = {
    var df_res = df
    aggTrans.foreach(f => {
      var transformationClusterPlan = transformClusterPlan(df_res, f)
      df_res = executeClusterTransformationPlan(f._1.split("\\$")(0), transformationClusterPlan, df_res)
    })
    return df_res
  }

  
    //execute a set of related transformations at once
  def executeClusterTransformationPlan(rootChange: String, clusterTransOps: List[(String, String)], df: DataFrame): DataFrame = {
    var highOrderQueryDefinition = HigherOrderFunctions(clusterTransOps)
    var df2 = df.withColumn("newCol", expr(highOrderQueryDefinition))
    df2 = df2.drop(df2.col(rootChange)).withColumn(rootChange.replaceAll("`", ""), df2.col("newCol"))
    return df2.drop("newCol")
  }

  
  
  // the global function that will generate a complete udf ready to execute
  def TransformDfBatch(df: DataFrame, colStructInput: List[List[(String, DataType)]]): DataFrame = {
    var df_result = df
    colStructInput.foreach(colEntry => {
      df_result = traverse("", colEntry, df_result)
    })
    return df_result
  }

  //traverse the diverse levels of nesting in a recursive way to apply transformation specified in TransformDatafarame function
  def traverse(path: String, cols: List[(String, DataType)], df: DataFrame): DataFrame = {

    this.newColsCounter += 1
    var current_path = path
    cols(0)._2 match {

      //currently we don't use this case
      case s: StructType => {
        if (current_path.size < 2) current_path = cols(0)._1
        else current_path = current_path + "." + cols(0)._1
        traverse(current_path, cols.tail, df)
      }

      case s: ArrayType => {

        current_path = current_path + "." + cols(0)._1

        //  case of transformation of a.b.c.d => a.b.c.[d]
        if (cols.size > 2) {

          var i = 0
          var nestedQuery = ""

          for (elt <- cols.tail.reverse) {
            var path = cols.view(0, cols.indexOf(elt)).map(x => x._1).mkString(".")

            var fieldsToKeep = writeSQL(cols(0)._2, path, elt._1, nestedQuery, it_list(i))
            if (i == cols.tail.size - 1) {
              nestedQuery = "transform(" + path.split("\\.").last + ", " +
                it_list(i) + "->  struct( " + fieldsToKeep.drop(1) + ") ) "
            } else {
              nestedQuery = "transform(" + it_list(i + 1) + "." + path.split("\\.").last + ", " +
                it_list(i) + "->  struct( " + fieldsToKeep.drop(1) + ")) "
            }

            i += 1
          }
         // println("query to perform !!! " + nestedQuery)
          var df4 = df.withColumn(cols(0)._1, expr(nestedQuery))
          return df4
        } //  case of transformation of a.b => a.[b]
        else {
         // println("query to perform !!! " + "easy format for one level")
          var query = writeSQL(cols(0)._2, cols(0)._1, cols(1)._1, "", "x")
          var df4 = df.withColumn(cols(0)._1, expr("transform(" + cols(0)._1 + ", x -> struct( " + query.drop(1) + ")  )"))

          return df4
        }
      }

      //we use it to poropagate the object id
      case s: MapType => {
        var setOfColsStructures = cols(0)._2.asInstanceOf[MapType].valueType.asInstanceOf[StructType].fields

        //simple case where we add a an object id to a field of first level
        if (cols(0)._1.split(",").size == 2) {
          var firstCol = cols(0)._1.split(",").head
          var secondCol = cols(0)._1.split(",").tail.last
          var SQL = writeSQL(setOfColsStructures.toList(0).dataType, firstCol, "", "", "x")

          var expressiontoPerform = "transform(" + firstCol + ", x -> struct( (x) as element," + secondCol + "))"
          if (SQL.trim().size > 3) {
            expressiontoPerform = "transform(" + firstCol + ", x -> struct(" + SQL.drop(1) + "," + secondCol + "))"
          }

          var df2 = df.withColumn("newCol_" + newColsCounter, expr(expressiontoPerform))
          df2 = df2.drop(df2.col(firstCol.trim())).withColumn(firstCol.trim().replaceAll("`", ""), df2.col("newCol_" + newColsCounter))
          return df2.drop("newCol_" + newColsCounter)

        } //complex case where we add a an object id to a field of nested level
        else {

          var setOfCols = cols(0)._1.split(",").view(0, cols(0)._1.split(",").size - 1)
          var secondCol = cols(0)._1.split(",").tail.last

          var i = 0
          var nestedQuery = ""

          var precedentElt = ""

          for (elt <- setOfCols.reverse) {
            var path = setOfCols.view(0, setOfCols.indexOf(elt) + 1).mkString(".")

            var indexStructure = 0
            var typeInferredforField = ""
            breakable {
              for (structFieldObj <- setOfColsStructures) {
                if (structFieldObj.name == elt) {
                  typeInferredforField = structFieldObj.dataType.toString()
                  break
                }
                indexStructure += 1
              }
            }
            var fieldsToKeep = writeSQL(setOfColsStructures(indexStructure).dataType, path, precedentElt, nestedQuery, it_list(i))
            if (typeInferredforField.startsWith("ArrayType")) {
              if (i == 0) {

                nestedQuery = "transform(" + it_list(i + 1) + "." + path.split("\\.").last + ", " +
                  it_list(i) + "->  struct( " + fieldsToKeep.drop(1) + "," + secondCol + " ) ) "

              } else if (i == setOfCols.size - 1) {
                nestedQuery = "transform(" + path.split("\\.").last + ", " +
                  it_list(i) + "->  struct( " + fieldsToKeep.drop(1) + ") ) "
              } else {
                nestedQuery = "transform(" + it_list(i + 1) + "." + path.split("\\.").last + ", " +
                  it_list(i) + "->  struct( " + fieldsToKeep.drop(1) + ")) "
              }

            } else {

              if (i == 0) {
                nestedQuery =
                  " struct( " + fieldsToKeep.drop(1).replaceAll("\\(" + it_list(i) + ".", "\\(" + it_list(i + 1) + "." + elt + ".") + "," + secondCol + " )  "

              } else {
                nestedQuery = " struct( " + fieldsToKeep.drop(1).replaceAll("\\(" + it_list(i) + ".", "\\(" + it_list(i + 1) + "." + elt + ".") + ") "
              }
            }
            precedentElt = elt
            i += 1
          }

          var expressiontoPerform = nestedQuery

         // println("query to perform !!! " + nestedQuery)
          var df2 = df.withColumn("newCol_" + newColsCounter, expr(expressiontoPerform))
          df2 = df2.drop(df2.col(setOfCols(0))).withColumn(setOfCols(0).replaceAll("`", ""), df2.col("newCol_" + newColsCounter))
          return df2.drop("newCol_" + newColsCounter)
        }
      }

      case _ => {
        val fieldsToKeep = df.select(explode(col(current_path)).as("ss")).select(col("ss.*")).columns
          .filter(_ != cols(0)._1)
          .map(n => "ss." + n)

        var df22 = df.withColumn("inter", struct(fieldsToKeep.head, fieldsToKeep.tail: _*))
        var df4 = df22.withColumn(current_path, expr("transform(" + current_path + ", x -> struct(inter.*,udfStringToArray (x." + cols(0)._1 + ") as " + cols(0)._1 + "  ) )"))

        return df4
      }

    }

  }

  // generate the schema of input columns that will be updated later
  // the result is List[List[(String, DataType)]]
  //   the first level corresponds to list of sources paths of each transformation
  // each source path of transformation can concern columns that belongs to different paths
  //that's why we have a second nested list
  def constructInputColumns(df: DataFrame, obj: TransformationDefinition): List[List[(String, DataType)]] = {
    var schema_input = ListBuffer[List[(String, DataType)]]()
    if (!obj.seqColumnTransformationDefinitions.isEmpty && obj.seqColumnTransformationDefinitions.size > 0) {
      for (elt <- obj.seqColumnTransformationDefinitions) {
        var ll_colTrans = ListBuffer[List[(String, DataType)]]()
        var oCol = elt.seqSourceDefinition(0).oPath.seqPathTokens(1).strToken
        var oColumnType = df.col(oCol).expr.dataType
        //case of String to Array
        if (elt.seqSourceDefinition.size == 1) {
          for (src <- elt.seqSourceDefinition) {
            if (src.oPath.seqPathTokens.size > 2) {
              var lisChildren = ListBuffer[String]()
              for (tok <- src.oPath.seqPathTokens) {
                if (!tok.strToken.equals("root")) {
                  oCol = tok.strToken
                  lisChildren += oCol
                }
              }
              var InferredTypes = generateSiblingsStructures(lisChildren(0), oColumnType, lisChildren)
              ll_colTrans += InferredTypes.toList
            } else {
              ll_colTrans += List[(String, DataType)]((oCol, oColumnType))
            }
          }
        } //case backwarding
        else {
          var pathsToTravers = ListBuffer[String]()
          var allcolnames = ""
          elt.seqSourceDefinition.map(x => {
            var seperatecols = ""
            x.oPath.seqPathTokens.map(f =>
              {
                if (!f.strToken.equals("root")) {
                  allcolnames = allcolnames + "," + f.strToken
                  seperatecols = seperatecols + "," + f.strToken
                }
              })
            pathsToTravers += seperatecols.drop(1)
          })
          var updatedCols = new ListBuffer[(String, DataType)]()

          //extractAllTypes
          var listChildren = ListBuffer[String]()
          listChildren ++= pathsToTravers(0).split(",").toList
          var inferredTypes = generateSiblingsStructures(listChildren(0), oColumnType, listChildren)
          var mapCol = ConstructMapForBackwarding(inferredTypes)
          var columnToParse = (allcolnames.drop(1), mapCol)
          updatedCols += columnToParse
          ll_colTrans += updatedCols.toList
        }
        schema_input ++= ll_colTrans
      }
    } else {
      throw new IllegalArgumentException("input columns not specified initially !!!")
    }
    return schema_input.toList
  }

  //construct a mapType instance where the first type refers to the objectID that we want
  //to propagate it, the second type refers to the structure where we want to inject the Object ID
  def ConstructMapForBackwarding(inferredTypes: ListBuffer[(String, DataType)]): MapType = {
    var typeToConstruct = ListBuffer[StructField]()
    for (elt <- inferredTypes) {
      var pair = StructField(elt._1, elt._2)
      typeToConstruct += pair
    }
    val mapCol = MapType(StringType, StructType(typeToConstruct.toArray))
    return mapCol
  }

  //starting from a field, we iterate all its siblings and we construct two list
  //list1: names of siblings, list2: structures of siblings
  def generateSiblingsStructures(oCol: String, expr: DataType, colsToSearch: ListBuffer[String]): ListBuffer[(String, DataType)] =
    {
      var res = ListBuffer[(String, DataType)]()
      var expressionToEvaluate = ListBuffer[StructField]()
      expressionToEvaluate += StructField(oCol, expr)

      while (!expressionToEvaluate.isEmpty) {
        var headEXP = expressionToEvaluate(0)
        if (colsToSearch.contains(headEXP.name)) {
          res += ((headEXP.name, headEXP.dataType))
          headEXP.dataType match {
            case StructType(s) => {
              s.map { y =>
                {
                  expressionToEvaluate += y
                }
              }
            }
            case ArrayType(s, b) => {
              expressionToEvaluate += StructField("element", s)
            }
            case _ => {
            }

          }
        } //this corresponds to the first descnendant of arraytype, usually they have the same name "" element
        else if (headEXP.name == "element" && headEXP.dataType.isInstanceOf[StructType]) {
          headEXP.dataType.asInstanceOf[StructType].fields.map { y =>
            {
              expressionToEvaluate += y
            }
          }
        }
        expressionToEvaluate -= headEXP
      }
      return res
    }

  //write the code of the higher order function that will be ibvoked to do the transformation
  def writeSQL(RootFieldType: DataType, path: String, col: String, fct: String, iterat: String): String = {
    var sql = ""
    var expressionToEvaluate = ListBuffer[DataType](RootFieldType)

    while (!expressionToEvaluate.isEmpty) {
      var headEXP = expressionToEvaluate(0)
      headEXP match {
        case StructType(s) => {
          var childrenNames = s.map { x => x.name }.toList
          // we do this check because df.col does not return datatypes correctly
          if (childrenNames.contains(path.split("\\.").last)) {

            s.foreach(n =>
              if (n.name == path.split("\\.").last) {
                expressionToEvaluate += n.dataType
              })
          } else {
            s.foreach(n =>
              if (n.name == col) {
                if (fct.size < 2) { sql = sql + ", udfStringToArray (" + iterat + "." + col + ") as " + col + " " }
                else {
                  sql = sql + ", " + fct + " as " + col + " "
                }
              } else { sql = sql + ",(" + iterat + "." + n.name + ") as " + n.name + " " })
          }
        }
        case ArrayType(s, b) => {
          expressionToEvaluate += s
        }
        case _ => {
        }
      }
      expressionToEvaluate -= headEXP
    }
    return sql
  }

  //write the code of the higher order function that will be ibvoked to do the transformation
  def writeSQLHigherOrderFunction(RootFieldType: DataType, currentCol: String, fct: String, previousCol: List[(String, String)], iterat: String): ListBuffer[(String, String)] = {
    var expressionToEvaluate = ListBuffer[DataType](RootFieldType)
    var transSET = ListBuffer[(String, String)]()
    while (!expressionToEvaluate.isEmpty) {
      var headEXP = expressionToEvaluate(0)
      headEXP match {
        case StructType(s) => {
          var childrenNames = s.map { x => x.name }.toList
          // we do this check because df.col does not return datatypes correctly
          if (childrenNames.contains(currentCol) && fct != "udf") {
            s.foreach(n =>
              if (n.name == currentCol) {
                expressionToEvaluate += n.dataType
              })
          } else {
            s.foreach(n =>
              if (n.name == currentCol) {
                if (fct == "udf") {
                  var trans = "udfStringToArray (" + iterat + "." + currentCol + ") as " + currentCol
                  transSET += ((currentCol, trans))
                }
              } else {
                var found = false
                if (previousCol != null) {
                  for (elt <- previousCol) {
                    if (n.name == elt._1) {
                      if (elt._2.contains("transform") || elt._2.contains("struct") || elt._2.contains("udfStringToArray")) {
                        transSET += ((n.name, elt._2))
                        found = true
                      }
                    }
                  }
                }
                if (!found) {
                  var trans = "(" + iterat + "." + n.name + ") as " + n.name
                  transSET += ((n.name, trans))
                }
              })
          }
        }
        case ArrayType(s, b) => {
          expressionToEvaluate += s
        }
        case _ => {
        }
      }
      expressionToEvaluate -= headEXP
    }
    return transSET
  }

  //higher order spark sql query return the set of transformations as a STRING
  def HigherOrderFunctions(transformationsRules: List[(String, String)]): String =
    {
      var sql = "  "
      var sqlPostponedtoTheEND = ""
      for (mapping <- transformationsRules) {
        if (mapping._1 == "backwarding") { sqlPostponedtoTheEND = mapping._2 }
        else { sql = sql + mapping._2 + "," }
      }
      if (!sqlPostponedtoTheEND.isEmpty()) {
        sql = sql + sqlPostponedtoTheEND + ","
      }
      sql = sql.dropRight(1)
      return sql
    }

  //traverse the diverse levels of nesting in a recursive way to apply transformation specified in TransformDatafarame function
  def transformClusterPlan(df: DataFrame, cluster: (String, PriorityQueue[InputTransformation])): List[(String, String)] = {
    var queueTransformation = cluster._2
    var elementToSearchNext = ""
    var previousNestinglevel = 0
    var plantransToDO = ListBuffer[(String, String)]()
    var previousColumn = ""
    var missingList = ListBuffer[String]()
    var setOfColsStructures = Array[StructField]()
    var setOfColsStructuresDifferents = Set[StructField]()
    var queueSingelton = (queueTransformation.size == 1)

    while (!queueTransformation.isEmpty) {
      var clusterToTransform = queueTransformation.dequeue()
      var cols = clusterToTransform.path.split("\\.").toList

/***************************new code*******************************/
 //update column name if we have the case "a.b" as a name of field
    if (clusterToTransform.path.contains("`")) {
        var colsSpe = new ListBuffer[String]()
        var splitSpecial = clusterToTransform.path.split("`")
        if (splitSpecial(0).contains("\\.")) {
          splitSpecial(0).split("\\.").map { x => colsSpe += x }
        }
        var complexeColumn = splitSpecial(1)
        colsSpe += "`" + complexeColumn + "`"
        if (splitSpecial.size>2)
        {var restofCOls=splitSpecial(2)
        if (restofCOls.split("\\.").size>1) {
          restofCOls.split("\\.").view(1,restofCOls.split("\\.").size).force.map { x => colsSpe += x }
        }
        }
        cols = List[String](colsSpe: _*)
      }

      var currentCOLTOtRANSFORM = cols.last
      var nextExpectedStructure = ""
      breakable {
        clusterToTransform.dt.head match {

          case s: ArrayType => {
            if (elementToSearchNext.isEmpty()) {
              var planTransformation = writeSQLHigherOrderFunction(clusterToTransform.dt(0), currentCOLTOtRANSFORM, "udf", null, it_list(clusterToTransform.depthlevel))
              plantransToDO = ListBuffer[(String, String)](planTransformation: _*)
              previousNestinglevel = clusterToTransform.depthlevel
            } /**elements of the same level**/ else if (previousNestinglevel == clusterToTransform.depthlevel) {
              var planTransformationUpdated = updateSQL(plantransToDO.toList, previousColumn, currentCOLTOtRANSFORM)
              plantransToDO = ListBuffer[(String, String)](planTransformationUpdated: _*)
              var query = HigherOrderFunctions(planTransformationUpdated)
            } /** we go up one level and we need to care about our change below **/ else if (currentCOLTOtRANSFORM == elementToSearchNext && cols.size > 1) {
              var planTransformation = writeSQLHigherOrderFunction(clusterToTransform.dt(0), currentCOLTOtRANSFORM, "udf", plantransToDO.toList, it_list(clusterToTransform.depthlevel))
              plantransToDO = planTransformation
              previousNestinglevel = clusterToTransform.depthlevel
            } /**closure**/ else {
              var currentCOLTOtRANSFORM = elementToSearchNext.split("\\.").last
              var planTransformation = writeSQLHigherOrderFunction(clusterToTransform.dt(0), currentCOLTOtRANSFORM, "udf", plantransToDO.toList, it_list(previousNestinglevel))
              var query = HigherOrderFunctions(planTransformation.toList)
              var trans = "transform(" + currentCOLTOtRANSFORM + "," + it_list(previousNestinglevel) + "-> struct(" + query + ") ) "
              plantransToDO = ListBuffer[(String, String)]((currentCOLTOtRANSFORM, trans))
            }
            /**update information for next round**/
            if (cols.size > 1) elementToSearchNext = cols.dropRight(1).last
            if (queueTransformation.isEmpty && cols.dropRight(1).size == 1) {
              var newLevelToParse = InputTransformation(0, cols.dropRight(1).mkString("."), clusterToTransform.dt)
              queueTransformation.enqueue(newLevelToParse)
            }
            previousColumn = currentCOLTOtRANSFORM
          }

          //we use it to propagate the object id
          case s: MapType => {
            setOfColsStructures = clusterToTransform.dt.head.asInstanceOf[MapType].valueType.asInstanceOf[StructType].fields
            setOfColsStructuresDifferents ++= clusterToTransform.dt.head.asInstanceOf[MapType].valueType.asInstanceOf[StructType].fields
            var objectID = cols.tail.last
            var fictifListCols = cols.view(0, cols.size - 1).force
            var currentCOLTOtRANSFORM = fictifListCols.last
            var currentColStructure = setOfColsStructures.last.dataType.toString()
            if (setOfColsStructures.last.name.contains("`")) {
              currentCOLTOtRANSFORM = setOfColsStructures.last.name
              fictifListCols = List[String](currentCOLTOtRANSFORM)
            }

            var trans = "" + objectID + " as ObjectId"

                 /***case 0***/
            if (clusterToTransform.depthlevel == 0) {
                // println(" /***case 0***/")
              var planTransformation = writeSQLHigherOrderFunction(df.col(currentCOLTOtRANSFORM).expr.dataType, currentCOLTOtRANSFORM, currentCOLTOtRANSFORM, plantransToDO.toList, it_list(previousNestinglevel - 1))
              trans = generateNestedHigherOrderFunction(planTransformation, clusterToTransform.dt.last, currentCOLTOtRANSFORM, previousNestinglevel - 1)
              plantransToDO = ListBuffer[(String, String)]((currentCOLTOtRANSFORM, trans))
            } else {
              // having the same depth level and the same father node
              /***case 2***/
              if (previousNestinglevel == clusterToTransform.depthlevel && elementToSearchNext == fictifListCols.dropRight(1).mkString(".")) {
                  //println(" /***case 2***/")
                var planTransformation = writeSQLHigherOrderFunction(setOfColsStructures.last.dataType, currentCOLTOtRANSFORM, currentCOLTOtRANSFORM, plantransToDO.toList, it_list(clusterToTransform.depthlevel))
                planTransformation += (("backwarding", trans))
                trans = generateNestedHigherOrderFunction(planTransformation, setOfColsStructures.last.dataType, currentCOLTOtRANSFORM, clusterToTransform.depthlevel)
                plantransToDO += ((currentCOLTOtRANSFORM, trans))
                plantransToDO ++= planTransformation

                  
              if (missingList.contains(fictifListCols.mkString("."))) {
                missingList = missingList.filter { x => x != fictifListCols.mkString(".") }
              }
              } 
              /***case 5***/
              else if (previousNestinglevel == clusterToTransform.depthlevel) {
                   //println(" /***case 5***/")
                var planTransformation = writeSQLHigherOrderFunction(setOfColsStructures.last.dataType, currentCOLTOtRANSFORM, currentCOLTOtRANSFORM, null, it_list(clusterToTransform.depthlevel))
                planTransformation += (("backwarding", trans))
                trans = generateNestedHigherOrderFunction(planTransformation, setOfColsStructures.last.dataType, currentCOLTOtRANSFORM, clusterToTransform.depthlevel)
                plantransToDO += ((currentCOLTOtRANSFORM, trans))
                elementToSearchNext = fictifListCols.dropRight(1).mkString(".")
                missingList += elementToSearchNext

              } // we go up one level and we need to care about our change below
              /***case 3***/
              else if (currentCOLTOtRANSFORM == elementToSearchNext.split("\\.").last && (!missingList.contains(fictifListCols.mkString(".")) || missingList.size == 0)) {  
                  //println(" /***case 3***/")
                var currentLevel = fictifListCols.size + 1
                var planTransformation = writeSQLHigherOrderFunction(setOfColsStructures.last.dataType, currentCOLTOtRANSFORM, "", plantransToDO.toList, it_list(currentLevel))
                planTransformation += (("backwarding", trans))
                trans = generateNestedHigherOrderFunctionWithClosure(planTransformation, setOfColsStructures.last.dataType, currentCOLTOtRANSFORM, currentLevel, fictifListCols.size)
                plantransToDO = ListBuffer[(String, String)]((currentCOLTOtRANSFORM, trans))
                
                if (setOfColsStructures.size > 1) {
                elementToSearchNext = fictifListCols.dropRight(1).mkString(".")
              }
              missingList = missingList.drop(missingList.indexOf(fictifListCols.mkString(".")))
             missingList += elementToSearchNext
              } 
          
              else if (elementToSearchNext.isEmpty() || !queueTransformation.isEmpty || queueSingelton) {         
                   //println(" /***case 1***/")
                var planTransformation = writeSQLHigherOrderFunction(setOfColsStructures.last.dataType, currentCOLTOtRANSFORM, currentCOLTOtRANSFORM, null, it_list(clusterToTransform.depthlevel))
                planTransformation += (("backwarding", trans))

                trans = generateNestedHigherOrderFunctionWithClosure(planTransformation, setOfColsStructures.last.dataType, currentCOLTOtRANSFORM, clusterToTransform.depthlevel, fictifListCols.size)

                if (fictifListCols.size > 1 && queueSingelton) {
                  // if (queueSingelton) {
                  var xx = InputTransformation(0, cols.dropRight(1).mkString("."), clusterToTransform.dt)
                  queueTransformation.enqueue(xx)
                  // }
                }
                plantransToDO += ((currentCOLTOtRANSFORM, trans))
                elementToSearchNext = fictifListCols.dropRight(1).mkString(".")
                missingList += elementToSearchNext
              } // clean my attributes, check if I didn#t miss the father of such node
             /***case 4***/
              else if (queueTransformation.isEmpty) {
                     //   println(" /***case 4***/")                    
                var find = missingList.contains(elementToSearchNext)
        
                if (find) {       
                   missingList=missingList.filter(p=> p!=elementToSearchNext)
                   }    
                
                var plantransToDOOld = List[(String, String)](plantransToDO: _*)
                var planTransformation = ListBuffer[(String, String)]()
                queueTransformation.enqueue(clusterToTransform)

                var inerList = ListBuffer[(String, String)]()

                missingList.foreach { f =>{
                   if(!f.isEmpty())
                  {
                 //    println ("elementMissing "+f)
                    var elementMissing = f.split("\\.").last                    
                    var missedColStructure = setOfColsStructuresDifferents.filter { x => x.name == elementMissing }.head.dataType //.toString()
                    planTransformation = writeSQLHigherOrderFunction(df.col(f).expr.dataType, elementMissing, "", plantransToDOOld, it_list(f.split("\\.").size + 1))
                    var missingLevel = f.split("\\.").size + 1
                    trans = generateNestedHigherOrderFunction(planTransformation, missedColStructure, elementMissing, missingLevel)
                    inerList += ((elementMissing, trans))
                  }}
                }
                plantransToDO ++= ListBuffer[(String, String)](inerList: _*)

                //empty missing list
                missingList = ListBuffer[String]()
                elementToSearchNext = fictifListCols.mkString(".")
                previousNestinglevel = previousNestinglevel
                break
              }
              //update
              if (setOfColsStructures.size > 1) { nextExpectedStructure = setOfColsStructures.dropRight(1).last.toString() }
              previousNestinglevel = clusterToTransform.depthlevel
              elementToSearchNext = fictifListCols.dropRight(1).mkString(".")
              if (missingList.contains(fictifListCols.mkString("."))) {
                missingList = missingList.filter { x => x != fictifListCols.mkString(".") }
              }
            }
          }
          case _ =>
            {
								/********************new code*****************/
 							plantransToDO = ListBuffer[(String, String)]((currentCOLTOtRANSFORM, "udfStringToArray("+currentCOLTOtRANSFORM+")"))
            }
            previousColumn = currentCOLTOtRANSFORM

        }
      }

    }

    return plantransToDO.toList
  }

  //generate a nested Higher order spark sql query with taking into account transformation to do in nested level
  def generateNestedHigherOrderFunction(planTransformation: ListBuffer[(String, String)], typeCol: DataType, currentCol: String, depthLevel: Int): String = {
    var trans = ""
    var query = HigherOrderFunctions(planTransformation.toList)
    if (typeCol.toString().startsWith("ArrayType")) {
      trans = "transform(" + it_list(depthLevel - 1) + "." + currentCol + "," + it_list(depthLevel) + "->struct(" + query + ") ) as " + currentCol
    } else {
      trans = "struct(" + query + ") as " + currentCol
      trans = trans.replaceAll("\\(" + it_list(depthLevel) + ".", "\\(" + it_list(depthLevel - 1) + "." + currentCol + ".")
    }
    return trans
  }

  //similar to previous function, just we add some condistions that garantue closure of our while loop
  def generateNestedHigherOrderFunctionWithClosure(planTransformation: ListBuffer[(String, String)], typeCol: DataType, currentCol: String, currentLevel: Int, sizeCols: Int): String = {
    var query = ""
    var transformation = HigherOrderFunctions(planTransformation.toList)
    if (typeCol.toString().startsWith("ArrayType")) {
      if (sizeCols == 1) {
        query = "transform(" + currentCol + "," + it_list(currentLevel) + "->struct(" + transformation + "))"
      } else {
        query = "transform(" + it_list(currentLevel - 1) + "." + currentCol + "," + it_list(currentLevel) + "->struct(" + transformation + ")) as " + currentCol
      }
    } 
//else {
//      /*var*/ query = "struct(" + transformation + ") as " + currentCol
//      query = query.replaceAll("\\(" + it_list(currentLevel) + ".", "\\(" + it_list(currentLevel - 1) + "." + currentCol + ".")
//    }
/****************new code********************/
 else {
      if (sizeCols == 1) {
              query = "struct(" + transformation + ") as " + currentCol
      query = query.replaceAll("\\(" + it_list(currentLevel) + ".", "\\(" +  currentCol + ".")
      } else {
      query = "struct(" + transformation + ") as " + currentCol
      query = query.replaceAll("\\(" + it_list(currentLevel) + ".", "\\(" + it_list(currentLevel - 1) + "." + currentCol + ".")
      } }
    return query
  }

  //update the code of an existing higher order function
  //update the transofrmstion of the currentCOl based on previous column's transformation
  def updateSQL(transSET: List[(String, String)], previousCol: String, currentCol: String): List[(String, String)] = {
    var transformationtoCopy = ""
    var transSETupdated = ListBuffer[(String, String)]()
    breakable {
      for (elt <- transSET) {
        if (elt._1 == previousCol) {
          transformationtoCopy = elt._2
          break
        }
      }
    }
    for (elt <- transSET) {
      if (elt._1 == currentCol) {
        var trans = transformationtoCopy.replaceAll(previousCol, currentCol)
        transSETupdated += ((elt._1, trans))
      } else { transSETupdated += ((elt._1, elt._2)) }
    }
    return transSETupdated.toList
  }

 
}


