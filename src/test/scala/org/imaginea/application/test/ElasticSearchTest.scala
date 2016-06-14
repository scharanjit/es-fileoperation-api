package org.imaginea.application.test

/**
  * Created by charanjits on 14/6/16.
  */

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.imaginea.application.csv.CSVProcessor
import org.imaginea.application.main.ElasticSearchApp._
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit

import scala.collection.mutable.ListBuffer
import org.junit.Test
import org.junit.Before

class ElasticSearchTest extends JUnitSuite with ShouldMatchersForJUnit with LazyLogging {

  var sb: StringBuilder = _
  var lb: ListBuffer[String] = _

  @Before def initialize() {
    sb = new StringBuilder("ScalaTest is ")
    lb = new ListBuffer[String]


  }

  @Test def testCSVExtractor = {
    logger.info("Inside testCSVExtractor")
    val folderPath = "/es-fileoperations-api/src/main/resources/"

    for (file <- new File(folderPath).listFiles().toIterator if file.getName endsWith ".csv") {
      logger.info(file.toString)

      val obj = new CSVProcessor()
      try {
        obj.CSVExtractor(file)
      } catch {
        case e: Exception => logger.error(" Error is :" + e)
      }
    }
  }

  @Test def verifyEasy() {
    // Uses ScalaTest assertions
    sb.append("easy!")
    assert(sb.toString === "ScalaTest is easy!")
    assert(lb.isEmpty)
    lb += "sweet"
    intercept[StringIndexOutOfBoundsException] {
      "concise".charAt(-1)
    }
  }

  @Test def verifyFun() {
    // Uses ScalaTest matchers
    sb.append("fun!")
    sb.toString should be("ScalaTest is fun!")
    lb should be('empty)
    lb += "sweet"
    evaluating {
      "concise".charAt(-1)
    } should produce[StringIndexOutOfBoundsException]
  }
}
