package de.mindlab.spark_schema_transformer.cli

import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import de.mindlab.spark_schema_transformer.row_iterator._
import de.mindlab.spark_schema_transformer.row_iterator.definitions._
import de.mindlab.spark_schema_transformer.column_iterator.example_definitions._

import java.nio.file.Paths
import java.io._
import de.mindlab.utils.PathAnalyzer
import scala.collection.mutable.ListBuffer
object ColumnIterator {

  def main(args: Array[String]): Unit = {

    val strAppName = "spark-schema-transformer (column iterator)"
    val strProlog = "Welcome to the " + strAppName
    var strMaster = "spark://columnizer-dev-1:7078" // "local[4]"
    var strSourcePathFs: String = "file://"
    var strSourcePath: String = "/bigdata/dlh/columnizer/_part_layout=cold/_part_granularity=86400"
    //var strSourcePath = "/bigdata/2020_03_02_gvv/columnizer/_part_layout=cold/_part_granularity=86400.ori/"    
    var basePath = strSourcePath + "/"
    var strWorkspacePathFs: String = "file://"
    var strWorkspacePath: String = "/bigdata/2020-02-22"
    var strTargetPathFs: String = "file://"
    var strTargetPath: String = "/bigdata/2020-02-25"

    println("")
    println(strProlog)
    println("")
    println("Usage: ")
    println("./bin/start_column_iterator_spark-web.sh <spark-master> <source fs> <source path> <workspace fs> <workspace path> <target fs> <target path> ")
    println("")
    println("example:")
    println("./bin/start_column_iterator_spark-web.sh spark://columnizer-dev-1:7078  file:///    /bigdata/bigdata_demo/columnizer.orig/_part_layout=cold/_part_granularity=86400  file:///       /bigdata/2020-02-28_w  file:///    /bigdata/2020-02-28_t")
    println("./bin/start_column_iterator_spark-web.sh <spark-master>                 <source fs> <source path>                                                                    <workspace fs> <workspace path>       <target fs> <target path>        ")
    println("")
    println("")
    println("Param args: ")
    args.foreach(println)
    println("")
    println("")

    if (args.size > 0) {
      strMaster = args(0)
    }
    if (args.size > 1) {
      strSourcePathFs = args(1)
    }

    //source path update 
    if (args.size > 2) {
      strSourcePath = args(2)
      basePath = strSourcePath + "/"
    }

    if (args.size > 3) {
      strWorkspacePathFs = args(3)
    }

    if (args.size > 4) {
      strWorkspacePath = args(4)
    }

    if (args.size > 5) {
      strTargetPathFs = args(5)
    }

    //target path update
    if (args.size > 6) {
      strTargetPath = args(6)
    }

    //if all params are given corectly
    if (args.size > 6) {

      println("strMaster:                           " + strMaster)
      println("")

      println("strSourcePathFs    strSourcePath:    " + strSourcePathFs + " " + strSourcePath)
      println("strWorkspacePathFs strWorkspacePath: " + strWorkspacePathFs + " " + strWorkspacePath)
      println("strTargetPath      strTargetPath:    " + strTargetPathFs + " " + strTargetPath)
      println("")
      println("")
      println("")
      println("")

      val spark = ColumnIterator.getSpark(strMaster, strAppName, "lib/spark-schema-transformer.jar")

      //register higher order functions
      spark.udf.register("udfStringToArray", (oRow: String) => {
        if (oRow == null || oRow.isEmpty()) { None }
        else { Some(Seq(oRow.toString())) }
      })

      // ColumnIterator.transform(spark, strSourcePath, strTargetPath)
      ColumnIterator.transform(spark, strSourcePathFs, strSourcePath, strWorkspacePathFs, strWorkspacePath, strTargetPathFs, strTargetPath)
    }

  }
  //-- main() ----------------------------------------------------------------------------------------------------------------------------------
  // def transform(spark: SparkSession, strSourcePath: String, strTargetPath: String) {
  def transform(spark: SparkSession, strSourcePathFs: String, strSourcePath: String, strWorkspacePathFs: String, strWorkspacePath: String, strTargetPathFs: String, strTargetPath: String) {
    /******************Loading******************/
    //  val transSET1 = ObjectIdTransformation.getFullTransformation("/opt/netmind/hbl/TransInput.txt")
    val transSET1 = ObjectIdTransformation.getFullTransformation("TransInput.txt")
    val transSET2 = ObjectIdTransformation.getStringToArrayTransformation()
    val mainTrans1 = TransformationDefinition(transSET1.toSeq)
    val mainTrans2 = TransformationDefinition(transSET2.toSeq)

    /****************************copy to HDFS**********************************/
    val oHadoopConf = new org.apache.hadoop.conf.Configuration()
    val oFsWorkspace = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(strWorkspacePathFs), oHadoopConf)
    val oFsTarget = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(strTargetPathFs), oHadoopConf)
    val bDeleteSrc = false

    /****************************Loading Data**********************************/
    val tsStartLoading: Long = System.currentTimeMillis
    val strReadUri = strSourcePathFs + strSourcePath
    println("de.mindlab.spark_schema_transformer.cli.transform(): " + "Val strReadUri: " + strReadUri)
    val df = spark.read.parquet(strReadUri)
    val tsEndLoading: Long = System.currentTimeMillis

    /******************* Preview [start]***********************/
    val tsStartPreview: Long = System.currentTimeMillis
    val seqDayTs = df.select("_part_ts").distinct.collect.map(x => { x.getInt(0) }).toSeq.sorted.reverse
    var seqTmpDf: Seq[(Int, org.apache.spark.sql.Dataset[org.apache.spark.sql.Row])] = Seq()

    seqDayTs.par.foreach(x => {
      val localDf = spark.read.parquet(strReadUri + "/_part_ts=" + x)
      val res = Seq((x, localDf))
      seqTmpDf.synchronized({ seqTmpDf = seqTmpDf ++ res })
    })
    val seqDf = seqTmpDf.sortBy(x => x._1).reverse
    val seqResDf = seqDf.map(x => { (x._1, x._2.inputFiles(0).split("/").reverse(0), x._2) })
    val tsEndPreview: Long = System.currentTimeMillis
    /******************* Preview [END]***********************/

    /******************initialization of Transformation******************/
    val tsStartTrans: Long = System.currentTimeMillis
    var df1 = seqResDf.head
    val colsInput = TransformationDefinition.constructInputColumns(df1._3, mainTrans1)
    val aggTrans = ObjectIdTransformation.organizeTransformation(colsInput, "backwarding")
    val transformationClusterPlan = TransformationDefinition.generateExecutionPlan(df, aggTrans)
    var df_res = TransformationDefinition.applyExecutionPlan(df1._3, transformationClusterPlan)

    val colsInput2 = TransformationDefinition.constructInputColumns(df_res, mainTrans2)
    val aggTrans2 = ObjectIdTransformation.organizeTransformation(colsInput2, "udf")
    val transformationClusterPlan2 = TransformationDefinition.generateExecutionPlan(df_res, aggTrans2)
    var df_final = TransformationDefinition.applyExecutionPlan(df_res, transformationClusterPlan2)



    //original schema to use it later:
    val oriCols = df1._3.columns.map(str => col("`" + str + "`"))

    //new columns in the new schemas      
    //    var newCols = df_final.columns.filter { x => !oriCols.contains(col(x)) }.map { x => col(x) }
    var newCols = df_final.columns.filter { x => !oriCols.contains(col("`" + x + "`")) }.map { x => col("`" + x + "`") }

    //new schema
    val newFormat = oriCols ++ newCols

    val tsEndTrans: Long = System.currentTimeMillis

    /******************Writing rest of documents******************/
    val tsStartWriting: Long = System.currentTimeMillis
    seqResDf.par.foreach(df => {
      var df_res = TransformationDefinition.applyExecutionPlan(df._3, transformationClusterPlan)
      var df_final = TransformationDefinition.applyExecutionPlan(df_res, transformationClusterPlan2)

      val strLocalWorkspacePath = strWorkspacePath + "/_part_ts=" + df._1
      val strLocalTargetPath = strTargetPath + "/_part_ts=" + df._1 // + "/" + x._2

      var df_ordered = df_final.select(newFormat: _*)

      df_ordered.repartition(df_ordered.col("_part_ts")).coalesce(1).write.mode(SaveMode.Overwrite).option("compression", "snappy").parquet(strWorkspacePathFs + strLocalWorkspacePath)
      val localDf = spark.read.parquet(strWorkspacePathFs + strLocalWorkspacePath)
      val strWorkspaceParquetFileName = localDf.inputFiles(0).split("/").reverse(0)

      val strSrcPath = strLocalWorkspacePath + "/" + strWorkspaceParquetFileName
      val strTgtPath = strLocalTargetPath + "/" + df._2

      FileUtil.copy(oFsWorkspace, new Path(strSrcPath), oFsTarget, new Path(strTgtPath), bDeleteSrc, oHadoopConf)
    })
    val tsEndWriting: Long = System.currentTimeMillis

    /*****************Results******************/
    val nDurationLoading: Long = tsEndLoading - tsStartLoading
    val nDurationTrans: Long = tsEndTrans - tsStartTrans
    val nDurationPreview: Long = tsEndPreview - tsStartPreview
    val nDurationWriting: Long = tsEndWriting - tsStartWriting

    println("Duration of loading:         " + (nDurationLoading / 1000.0) + " sec.")
    println("Duration of transformations: " + (nDurationTrans / 1000.0) + " sec.")
    println("Duration of preview:         " + (nDurationPreview / 1000.0) + " sec.")
    println("Duration of writing:         " + (nDurationWriting / 1000.0) + " sec.")
  }

  def getSpark(strMaster: String, strAppName: String, strJars: String): SparkSession =
    {

      val spark = SparkSession.builder
        .master(strMaster)
        .appName(strAppName)
        .config("spark.jars", strJars)
        //      .config("spark.app.name",                                           strAppName)
  .config("spark.local.dir", "/opt/netmind/lne/spark-schema-transformer/spark-warehouse/")
          //.config("spark.local.dir", "/bigdata/spark-warehouse/")
 .config("spark.cores.max", "24")
        .config("spark.default.parallelism", "10")
        .config("spark.driver.maxResultSize", "32G")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executor.cores", "6")
        .config("spark.executor.heartbeatInterval", "60")
        .config("spark.executor.memory", "25g")
        .config("spark.executor.instances", "4")
        .config("spark.scheduler.revive.interval", "300s")
        .config("spark.scheduler.listenerbus.eventqueue.capacity", "1000")
        .config("spark.scheduler.mode", "FIFO")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.files.maxPartitionBytes", "104857600")
        .config("spark.sql.files.openCostInBytes", "104857600")
        .config("spark.sql.shuffle.partitions", "30")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "30")
        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "12")
        .config("spark.submit.deployMode", "client")
        .getOrCreate()

      spark
    }

}
