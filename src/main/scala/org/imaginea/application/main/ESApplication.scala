package org.imaginea.application.main

/**
  * Created by charanjits on 8/6/16.
  */


import com.typesafe.scalalogging.LazyLogging
import org.imaginea.application.csv.CSVProcessor
import java.io.File


object ESApplication extends LazyLogging {


  def main(args: Array[String]) {
    logger.info("in main")
    //    val folderPath= args(0)
    //    val folderPath="" //testing null
    val folderPath = "/home/charanjits/Documents/ideaProjects/es-fileoperations-api/src/main/resources/"

    logger.info(folderPath.toString)


    folderPath match {
      case (folderPath) if (!folderPath.isEmpty) => fileFinder(folderPath)
      case _ => logger.info("Enter a Valid Folder Path for CSV files")
    }

  }



  def fileFinder(path: String) = {
    logger.info("Inside fileFinder")
    for (file <- new File(path).listFiles().toIterator if file.getName endsWith ".csv") {
      logger.info(file.toString)
      val obj = new CSVProcessor()
      obj.CSVExtractor(file)
    }
  }



}
