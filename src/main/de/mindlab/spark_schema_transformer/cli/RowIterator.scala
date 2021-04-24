package de.mindlab.spark_schema_transformer.cli


import de.mindlab.spark_schema_transformer.row_iterator._
import de.mindlab.spark_schema_transformer.row_iterator.definitions._

import de.mindlab.spark_schema_transformer.row_iterator.example_definitions._

import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


object RowIterator {
  
  def main(args: Array[String]): Unit = {
    
    val strAppName = "spark-schema-transformer (row iterator)"
    val strProlog = "Welcome to the " + strAppName
    var strMaster = "spark://columnizer-dev-1:7078" // "local[4]"
    
    var strSourcePathFs:    String = "file://"
    var strSourcePath:      String = "/bigdata/dlh/columnizer/_part_layout=cold/_part_granularity=86400"
    var strWorkspacePathFs: String = "file://"
    var strWorkspacePath:   String = "/bigdata/2020-02-22"
    var strTargetPathFs:    String = "file://"
    var strTargetPath:      String = "/bigdata/2020-02-25"
    
    
    println( "" )
    println( strProlog )
    println( "" )
    println( "Usage: " )
    println( "./bin/start_row_iterator_spark-web.sh <spark-master> <source fs> <source path> <workspace fs> <workspace path> <target fs> <target path> " )
    println( "" )
    println( "example:" )
    println( "./bin/start_row_iterator_spark-web.sh spark://columnizer-dev-1:7078  file:///    /bigdata/bigdata_demo/columnizer.orig/_part_layout=cold/_part_granularity=86400  file:///       /bigdata/2020-02-28_w  file:///    /bigdata/2020-02-28_t" )
    println( "./bin/start_row_iterator_spark-web.sh <spark-master>                 <source fs> <source path>                                                                    <workspace fs> <workspace path>       <target fs> <target path>        " )
    println( "" )
    println( "" )
    println( "Param args: " )
    args.foreach( println )
    println( "" )
    println( "" )
    
    if ( args.size >0 )
    {
      strMaster = args(0)
    }
    if ( args.size >1 )
    {
      strSourcePathFs = args(1)
    }
    
    if ( args.size >2 )
    {
      strSourcePath = args(2)
    }
    
    if ( args.size >3 )
    {
      strWorkspacePathFs = args(3)
    }
    
    if ( args.size >4 )
    {
      strWorkspacePath = args(4)
    }
    
    if ( args.size >5 )
    {
      strTargetPathFs = args(5)
    }
    
    if ( args.size >6 )
    {
      strTargetPath = args(6)
    }
    if ( args.size >6 )
    {
    
      //val oFs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("file://"), new org.apache.hadoop.conf.Configuration())
  
      println( "strMaster:                           " + strMaster    )
      println( "" )
      
      println( "strSourcePathFs    strSourcePath:    " + strSourcePathFs    + " " + strSourcePath    )
      println( "strWorkspacePathFs strWorkspacePath: " + strWorkspacePathFs + " " + strWorkspacePath )
      println( "strTargetPath      strTargetPath:    " + strTargetPathFs    + " " + strTargetPath    )
      println( "" )
      println( "" )
      println( "" )
      println( "" )
      
      val spark = RowIterator.getSpark( strMaster, strAppName, "lib/spark-schema-transformer.jar" )
      
      
        
        
      val strVersion = spark.version
      
      println( "Spark version " + strVersion )
      
  //    Thread.sleep(30000L)
      
      RowIterator.transform( spark, strSourcePathFs, strSourcePath, strWorkspacePathFs, strWorkspacePath, strTargetPathFs, strTargetPath )
    
    }
    else
    {
      println( "ERROR: missing arguments. Need 7 but get " + args.size + ". See Usage!")
    }
      
      
  }//-- main() -----------------------------------------------------------------------------------------------------------------------------------
  
  
  
  
  def getSpark( strMaster: String, strAppName: String, strJars:String ): SparkSession =
  {
    
    val spark = SparkSession.builder
      .master(strMaster)
      .appName(strAppName)
      .config("spark.jars",                                               strJars)
//      .config("spark.app.name",                                           strAppName)
      .config("spark.local.dir",                                          "/opt/netmind/lne/spark-schema-transformer/spark-warehouse/")
      .config("spark.cores.max",                                          "24")
      .config("spark.default.parallelism",                                "10")
      .config("spark.driver.maxResultSize",                               "32G")
      .config("spark.dynamicAllocation.enabled",                          "false")
      .config("spark.executor.cores",                                     "6")
      .config("spark.executor.heartbeatInterval",                         "60")
      .config("spark.executor.memory",                                    "25g")
      .config("spark.executor.instances",                                 "4" )
      .config("spark.scheduler.revive.interval",                          "300s" )
      .config("spark.scheduler.listenerbus.eventqueue.capacity",          "1000")
      .config("spark.scheduler.mode",                                     "FIFO")
      .config("spark.shuffle.service.enabled",                            "true")
      .config("spark.sql.caseSensitive",                                  "true")
      .config("spark.sql.files.maxPartitionBytes",                        "104857600")
      .config("spark.sql.files.openCostInBytes",                          "104857600")
      .config("spark.sql.shuffle.partitions",                             "30")
      .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "30")
      .config("spark.sql.sources.parallelPartitionDiscovery.threshold",   "12")
      .config("spark.submit.deployMode",                                  "client")
      .getOrCreate()
    
    
    spark
  }
  
  
  
  def transform( spark: SparkSession, strSourcePathFs: String, strSourcePath: String, strWorkspacePathFs: String, strWorkspacePath: String, strTargetPathFs: String, strTargetPath: String )
  {
    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "START" )
    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "Param spark:              " + spark )
    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "Param strSourcePathFs:    " + strSourcePathFs )
    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "Param strSourcePath:      " + strSourcePath )
    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "Param strWorkspacePathFs: " + strWorkspacePathFs )
    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "Param strWorkspacePath:   " + strWorkspacePath )
    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "Param strTargetPathFs:    " + strTargetPathFs)
    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "Param strTargetPath:      " + strTargetPath)
    
    val oTransformationDefinition: TransformationDefinition = ObjectIdTransformation.getFullTransformation


//val strSourcePath    = "/bigdata/dlh/columnizer/_part_layout=cold/_part_granularity=86400"
//val strWorkspacePath = "/bigdata/2020-02-22"
//val strTargetPath    = "/bigdata/2020-02-25"
    
    
val tsStartLoading: Long         = System.currentTimeMillis

val strReadUri = strSourcePathFs + strSourcePath

    println( "de.mindlab.spark_schema_transformer.cli.transform(): " + "Val strReadUri: " + strReadUri)



val df = spark.read.parquet( strReadUri ) //.filter( $"_part_ts" >= 1565568000 ).filter( $"_part_ts" <= 1565913600 )

val seqDayTs = df.select("_part_ts").distinct.collect.map(x => {x.getInt(0)}).toSeq.sorted.reverse

var seqTmpDf: Seq[ ( Int, org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] ) ] = Seq()

seqDayTs.par.foreach(x => { 
  
  val localDf = spark.read.parquet( strReadUri + "/_part_ts=" + x)
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

val oHadoopConf  = new org.apache.hadoop.conf.Configuration()
val oFsWorkspace = org.apache.hadoop.fs.FileSystem.get( new java.net.URI(strWorkspacePathFs), oHadoopConf )
val oFsTarget    = org.apache.hadoop.fs.FileSystem.get( new java.net.URI(strTargetPathFs),    oHadoopConf )
val bDeleteSrc   = false

seqResDf.par.foreach( x => {
  val strLocalWorkspacePath = strWorkspacePath + "/_part_ts=" + x._1
  val strLocalTargetPath    = strTargetPath    + "/_part_ts=" + x._1 // + "/" + x._2
  
  x._3.repartition(x._3.col("_part_ts")).coalesce(1).write.mode(SaveMode.Overwrite).option("compression", "snappy").parquet( strWorkspacePathFs + strLocalWorkspacePath )
  val localDf = spark.read.parquet( strWorkspacePathFs + strLocalWorkspacePath )
  val strWorkspaceParquetFileName = localDf.inputFiles(0).split("/").reverse(0)
  
  val strSrcPath = strLocalWorkspacePath + "/" + strWorkspaceParquetFileName
  val strTgtPath = strLocalTargetPath + "/" + x._2
  
  //oFs.rename(new Path( strSrcPath ), new Path( strTgtPath ));
  FileUtil.copy(oFsWorkspace, new Path( strSrcPath ), oFsTarget, new Path( strTgtPath ), bDeleteSrc, oHadoopConf)
} )


val tsEndWriting: Long         = System.currentTimeMillis

val nDurationWriting: Long = tsEndWriting - tsStartWriting

println( "Duration of loading:         " + (nDurationLoading/1000.0) + " sec." )
println( "Duration of transformations: " + (nDurationTrans/1000.0)   + " sec." )
println( "Duration of preview:         " + (nDurationPreview/1000.0) + " sec." )
println( "Duration of writing:         " + (nDurationWriting/1000.0) + " sec." )

  }
  
  
  
  
  
  def check( spark: SparkSession, seqPrimaryKeyColumnNames: Seq[String], strSourcePathFs: String, strSourcePath: String, strTargetPathFs: String, strTargetPath: String, seqTimestamps: Seq[String] ): Unit =
  {
    println( "de.mindlab.spark_schema_transformer.cli.check(): " + "START" )
    println( "de.mindlab.spark_schema_transformer.cli.check(): " + "Param spark:                     " + spark )
    println( "de.mindlab.spark_schema_transformer.cli.check(): " + "Param seqPrimaryKeyColumnNames:  " + seqPrimaryKeyColumnNames )
    println( "de.mindlab.spark_schema_transformer.cli.check(): " + "Param strSourcePathFs:           " + strSourcePathFs )
    println( "de.mindlab.spark_schema_transformer.cli.check(): " + "Param strSourcePath:             " + strSourcePath )
    println( "de.mindlab.spark_schema_transformer.cli.check(): " + "Param strTargetPathFs:           " + strTargetPathFs)
    println( "de.mindlab.spark_schema_transformer.cli.check(): " + "Param strTargetPath:             " + strTargetPath)
    println( "de.mindlab.spark_schema_transformer.cli.check(): " + "Param seqTimestamps:             " + seqTimestamps)
    
    val oTransformationDefinition: TransformationDefinition = ObjectIdTransformation.getFullTransformation


    
    val tsStartLoading: Long         = System.currentTimeMillis


    val df = spark.read.parquet( strSourcePathFs + strSourcePath )
    
    
    
    var seqDayTs: Seq[Int]= Seq()  
    
    if ( 0 == seqTimestamps.length )
    {
      seqDayTs = df.select("_part_ts").distinct.collect.map(x => {x.getInt(0)}).toSeq.sorted.reverse
    }
    else
    {
      seqDayTs = seqTimestamps.map( x => { x.toInt } )
    }
    
    
    
    
    
    var seqTmpDf: Seq[ ( Int, org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ], org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] ) ] = Seq()
    
    seqDayTs.par.foreach(x => { 
      
      val originalDf    = spark.read.parquet( strSourcePathFs + strSourcePath + "/_part_ts=" + x)
      val transformedDf = spark.read.parquet( strTargetPathFs + strTargetPath + "/_part_ts=" + x)
      val res           = Seq( (x, originalDf, transformedDf ))
      
      seqTmpDf.synchronized({     seqTmpDf = seqTmpDf ++ res })
       
    })
    
    val seqDf = seqTmpDf.sortBy( x => x._1 ).reverse
    
    val tsEndLoading: Long         = System.currentTimeMillis
    
    
    
    
    
    val tsStartTrans: Long         = System.currentTimeMillis
    
    val oElementFirst: ( Int, org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ], org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] ) = seqDf.head
//    val oElementLast:  ( Int, org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ], org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ] ) = seqDf.reverse.head
    
    val oFirstOriginalDf    = oElementFirst._2
    val oFirstTransformedDf = oElementFirst._3
    
    val oSchemaDiff = Comparator.getSchemaDiff( oFirstOriginalDf, oFirstTransformedDf )
    
    
    //val seqTuplesToTest = Seq( oElementFirst, oElementLast )
    
    
//    compare( 
//      seqPrimaryKeyColumnNames: Seq[String],
//      oSchemaDiff:              SchemaDiff,
//      oOriginalDf:              org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ], 
//      oTransformedDf:           org.apache.spark.sql.Dataset[ org.apache.spark.sql.Row ],
//      nRows:                    Int = 10)
    
    
    
    seqDf.foreach( oTuple => {
      
      val nBlockTs       = oTuple._1
      val oOriginalDf    = oTuple._2
      val oTransformedDf = oTuple._3
      
      
//      case class ValueDiff(
//        bPrimaryKeyIsOkOriginal:      Boolean,
//        bPrimaryKeyIsOkTransformed:   Boolean,
//        bUnchangedColumnsEqual:       Boolean,
//        seqDifferentUnchangedColumns: Seq[StructType]
//      )
      
      
      val oValueDiffTuple: (ValueDiff, String, String) = Comparator.compare( seqPrimaryKeyColumnNames, oSchemaDiff, oOriginalDf, oTransformedDf, 50 )
      
      
      
      //oValueDiff
      
      if ( !( oValueDiffTuple._1.bPrimaryKeyIsOkOriginal && oValueDiffTuple._1.bPrimaryKeyIsOkTransformed && oValueDiffTuple._1.bUnchangedColumnsEqual ) )
      {
        println( "de.mindlab.spark_schema_transformer.cli.check(): " + "ERROR: Unchanged columns are not eual for Ts " + nBlockTs + "." )
        println( "de.mindlab.spark_schema_transformer.cli.check(): " + "ERROR: Var oValueDiffTuple._1.bPrimaryKeyIsOkOriginal:    " + oValueDiffTuple._1.bPrimaryKeyIsOkOriginal + "." )
        println( "de.mindlab.spark_schema_transformer.cli.check(): " + "ERROR: Var oValueDiffTuple._1.bPrimaryKeyIsOkTransformed: " + oValueDiffTuple._1.bPrimaryKeyIsOkTransformed + "." )
        println( "de.mindlab.spark_schema_transformer.cli.check(): " + "ERROR: Var oValueDiffTuple._1.bUnchangedColumnsEqual:     " + oValueDiffTuple._1.bUnchangedColumnsEqual + "." )
        println( "===================================================================================================" )
      }
      else
      {
        println( "de.mindlab.spark_schema_transformer.cli.check(): " + "Dissapeared columns for Ts " + nBlockTs + ":" )
        println( oValueDiffTuple._2 )
        println( "===================================================================================================" )
        println( "de.mindlab.spark_schema_transformer.cli.check(): " + "New columns for Ts " + nBlockTs + ":" )
        println( oValueDiffTuple._3 )
        println( "===================================================================================================" )
      }
      
    })
    
    
    
    val tsEndTrans: Long         = System.currentTimeMillis
    
    
    
    val nDurationLoading: Long = tsEndLoading - tsStartLoading
    val nDurationTrans:   Long = tsEndTrans   - tsStartTrans
    
    println( "Check Duration of loading:         " + (nDurationLoading/1000.0) + " sec." )
    println( "Check Duration of check:           " + (nDurationTrans/1000.0) + " sec." )
    
    
  }
  
  
  
  
  
}