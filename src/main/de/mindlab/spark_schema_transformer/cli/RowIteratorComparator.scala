package de.mindlab.spark_schema_transformer.cli

object RowIteratorComparator {
  
  
  def main(args: Array[String]): Unit = {
    
    val strAppName = "Comparator for spark-schema-transformer (row iterator)"
    val strProlog = "Welcome to the " + strAppName
    var strMaster = "spark://columnizer-dev-1:7078" // "local[4]"
    
    var strSourcePathFs:    String = "file://"
    var strSourcePath:      String = "/bigdata/dlh/columnizer/_part_layout=cold/_part_granularity=86400"
    var strWorkspacePathFs: String = "file://"
    var strWorkspacePath:   String = "/bigdata/2020-02-22"
    var strTargetPathFs:    String = "file://"
    var strTargetPath:      String = "/bigdata/2020-02-25"
    
    var bCompareAll = false
    
    
    println( "" )
    println( strProlog )
    println( "" )
    println( "Usage: " )
    println( "./bin/start_row_iterator_comparator_spark-web.sh <spark-master> <source fs> <source path> <target fs> <target path> <columns representing a primary key> [<Timestamps to compare>]" )
    println( "" )
    println( "example:" )
    println( "./bin/start_row_iterator_comparator_spark-web.sh spark://columnizer-dev-1:7078  file:///    /bigdata/bigdata_demo/columnizer.orig/_part_layout=cold/_part_granularity=86400    file:///    /bigdata/2020-02-28_t   \"nm_click_id,nm_pageView_id\"       \"1578960000\"              " )
    println( "./bin/start_row_iterator_comparator_spark-web.sh <spark-master>                 <source fs> <source path>                                                                      <target fs> <target path>           <columns representing a primary key>  [<Timestamps to compare>]" )
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
      strTargetPathFs = args(3)
    }
    
    if ( args.size >4 )
    {
      strTargetPath = args(4)
    }
    
    var seqPrimaryKeyColumnNames: Seq[String] = Seq()
    var seqTimeStamps:            Seq[String] = Seq()
    
    if ( args.size >5 )
    {
      val strPrimaryKeyCsv = args(5)
      
      seqPrimaryKeyColumnNames = strPrimaryKeyCsv.split(",").toSeq.map(x => {x.trim})
      
    }
    
    
    if ( args.size >6 )
    {
      val strTimeStampsCsv = args(6)
      
      seqTimeStamps = strTimeStampsCsv.split(",").toSeq.map(x => {x.trim})
    }
    
    
    if ( args.size >5 )
    {
    
      //val oFs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("file://"), new org.apache.hadoop.conf.Configuration())
  
      println( "strMaster:                           " + strMaster    )
      println( "" )
      
      println( "strSourcePathFs    strSourcePath:    " + strSourcePathFs    + " " + strSourcePath    )
      println( "strTargetPath      strTargetPath:    " + strTargetPathFs    + " " + strTargetPath    )
      
      bCompareAll = 0 == seqTimeStamps.length
      
      if ( bCompareAll )
      {
        println( "Should compare all blocks. This could be expensive."    )
      }
      else
      {
        println( "Should compare for the following timestamps: " + seqTimeStamps )
      }
      
      println( "" )
      println( "" )
      println( "" )
      println( "" )
      
      
      
      val spark = RowIterator.getSpark( strMaster, strAppName, "lib/spark-schema-transformer.jar" )
        
        
      val strVersion = spark.version
      
      println( "Spark version " + strVersion )
      
      
      RowIterator.check( spark, seqPrimaryKeyColumnNames, strSourcePathFs, strSourcePath, strTargetPathFs, strTargetPath, seqTimeStamps )
    }
    else
    {
      println( "ERROR: missing arguments. Need 5 but get " + args.size + ". See Usage!")
    }
      
      
  }//-- main() --------------------------------------------
  
  

}