package org.imaginea.application.main

/**
  * Created by charanjits on 8/6/16.
  */


import com.typesafe.scalalogging.LazyLogging
import org.imaginea.application.csv.CSVProcessor
import java.io.File


object ElasticSearchApp extends LazyLogging {


  def main(args: Array[String]) {
    logger.info("in main")
    try {
      val folderPath = args(0)
      logger.info(folderPath.toString)
      val count = fileFinder(folderPath)
      logger.info("=== " + count + " CSV files Uploaded===")
    } catch {
      case e: ArrayIndexOutOfBoundsException => logger.error("Enter a Valid Folder Path for CSV files")
    }

  }


  def fileFinder(path: String): Int = {
    var count = 0
    logger.info("Inside fileFinder")
    for (file <- new File(path).listFiles.toIterator if file.getName endsWith ".csv") {
      logger.info(file.toString)
      val obj = new CSVProcessor()
      obj.CSVExtractor(file)
      count = count + 1
    }
    count
  }


}
