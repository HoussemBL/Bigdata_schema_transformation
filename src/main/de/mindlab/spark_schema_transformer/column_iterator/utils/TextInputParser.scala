package de.mindlab.spark_schema_transformer.column_iterator.utils
import de.mindlab.spark_schema_transformer.row_iterator.definitions._
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

case class TextInputParser(filePath: String) {

}

object TextInputParser {

  /*parse a txt file line by line and automatically consider them as inputs for 
 transformation  of type object id propagation  */
  def parser(obj: TextInputParser): ListBuffer[ColumnTransformationDefinition] = {
    var seqTrs = ListBuffer[ColumnTransformationDefinition]()

    val pTRoot = PathToken("root")
    val pT3 = PathToken("nm_pageView_id")
    val seq3 = Seq[PathToken](pTRoot, pT3)
    val path3 = PathDefinition(seq3)
    val src3 = SourceDefinition(path3)
    var i = 0

val stream= this.getClass.getClassLoader.getResourceAsStream("TransInput.txt")
val lines: Iterator[String] = Source.fromInputStream( stream ).getLines

    breakable {
      for (line <- lines/*Source.fromFile(obj.filePath).getLines*/) {
        var seq1 = ListBuffer[PathToken](pTRoot)
        if (!line.contains("`")) {
          var columns = line.split("\\.")

          for (colname <- columns) {
            var pTok = PathToken(colname.trim())
            seq1 += pTok

          }
        } else {
          seq1 += PathToken(line.trim())

        }
        var path1 = PathDefinition(seq1)

        var src1 = SourceDefinition(path1)
        var seqSrc1 = Seq[SourceDefinition](src1, src3)
        var colTrans1 = ColumnTransformationDefinition(path1, "propagateID", seqSrc1, true)

        seqTrs += colTrans1
        i += 1
        //if (i == 40) { break }
      }
    }

    return seqTrs
  }
}