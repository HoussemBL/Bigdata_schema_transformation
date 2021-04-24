package de.mindlab.utils

import java.io._

class PathAnalyzer {
  
}


object PathAnalyzer{
  
  //return set of paths
    def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory()).toList
    } else {
      List[File]()
    }
  }
}