import de.mindlab.spark_schema_transformer.row_iterator._
import de.mindlab.spark_schema_transformer.row_iterator.definitions._

import de.mindlab.spark_schema_transformer.row_iterator.example_definitions._

import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs._


val oTransformationDefinition: TransformationDefinition = ObjectIdTransformation.getFullTransformation


val strSourcePath    = "/bigdata/dlh/columnizer/_part_layout=cold/_part_granularity=86400"
val strWorkspacePath = "/bigdata/2020-02-22"
val strTargetPath    = "/bigdata/2020-02-25"
    
    
val tsStartLoading: Long         = System.currentTimeMillis

val df = spark.read.parquet(strSourcePath) //.filter( $"_part_ts" >= 1565568000 ).filter( $"_part_ts" <= 1565913600 )

val seqDayTs = df.select("_part_ts").distinct.collect.map(x => {x.getInt(0)}).toSeq.sorted.reverse

var seqTmpDf: Seq[ ( Int, org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] ) ] = Seq()

seqDayTs.par.foreach(x => { 
  
  val localDf = spark.read.parquet( strSourcePath + "/_part_ts=" + x)
  val res     = Seq( (x, localDf ))
  seqTmpDf.synchronized({     seqTmpDf = seqTmpDf ++ res })
   
  })

val seqDf = seqTmpDf.sortBy( x => x._1 ).reverse

val tsEndLoading: Long         = System.currentTimeMillis



val tsStartTrans: Long         = System.currentTimeMillis

val prepareRes = SchemaMerger.prepareTransformation(seqDf.head._2, oTransformationDefinition)

//val seqResDf = seqDf.map( x => { (x._1, x._2.inputFiles(0).split("/").reverse(0), SchemaMerger.transformDf( x._2, oTransformationDefinition )) })
val seqResDf = seqDf.map( x => { (x._1, x._2.inputFiles(0).split("/").reverse(0), SchemaMerger.innerTransformDf( x._2, prepareRes._1, prepareRes._2 )) })

val tsEndTrans: Long         = System.currentTimeMillis



val tsStartPreview: Long         = System.currentTimeMillis

val tsEndPreview: Long         = System.currentTimeMillis



val nDurationLoading: Long = tsEndLoading - tsStartLoading
val nDurationTrans:   Long = tsEndTrans   - tsStartTrans
val nDurationPreview: Long = tsEndPreview - tsStartPreview

println( "Duration of loading:         " + (nDurationLoading/1000.0) + " sec." )
println( "Duration of transformations: " + (nDurationTrans/1000.0) + " sec." )
println( "Duration of preview:         " + (nDurationPreview/1000.0) + " sec." )


val tsStartWriting: Long         = System.currentTimeMillis


val oFs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("file:://"), new org.apache.hadoop.conf.Configuration())


seqResDf.par.foreach( x => {
  val strLocalWorkspacePath = strWorkspacePath + "/_part_ts=" + x._1
  val strLocalTargetPath    = strTargetPath    + "/_part_ts=" + x._1 // + "/" + x._2
  
  x._3.repartition($"_part_ts").coalesce(1).write.mode(SaveMode.Overwrite).option("compression", "snappy").parquet( strLocalWorkspacePath )
  val localDf = spark.read.parquet( strLocalWorkspacePath )
  val strWorkspaceParquetFileName = localDf.inputFiles(0).split("/").reverse(0)
  
  val strSrcPath = strLocalWorkspacePath + "/" + strWorkspaceParquetFileName
  val strTgtPath = strLocalTargetPath + "/" + x._2
  
  oFs.rename(new Path( strSrcPath ), new Path( strTgtPath ));
  
} )


val tsEndWriting: Long         = System.currentTimeMillis

val nDurationWriting: Long = tsEndWriting - tsStartWriting

println( "Duration of writing:         " + (nDurationWriting/1000.0) + " sec." )


