package org.imaginea.application.csv

/**
  * Created by charanjits on 9/6/16.
  */

import java.io.{File, InputStream}

import com.typesafe.scalalogging.LazyLogging
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.conn.HttpHostConnectException


class CSVProcessor extends LazyLogging {

  case class SampleEvents(eventNo: String, appType: String, eventDate: String, logDate: String, deviceObjID: String, devChildObjID: String, itemNo: String, subItemNo: String, eventID: String, priorityID: String, isAlarm: String, siteID: String, eventDescription: String, siteDescription: String, deviceDescription: String, devChildDescription: String, itemDescription: String, priorityDescription: String, ackOperator: String, ackComment: String, ackDate: String, procOperator: String, procComment: String, procDate: String, categoryID: String, categoryDescription: String, classiID: String, classiDesc: String, iEventDate: String, iLogDate: String, empCode: String, deviceTime: String, revision: String)

  case class FileOutput(empCode: String, inDate: String, outDate: String, totalHours: String, siteDescription: String, itemDescription: String)

  val EMPCODE = "empCode"
  val INDATE = "inDate"
  val OUTDATE = "outDate"
  val TOTALHOURS = "totalHours"
  val SITEDESCRIPTION = "siteDescription"
  val ITEMDESCRIPTION = "itemDescription"


  val EVENTNO = "eventNo"
  val APPTYPE = "appType"
  val EVENTDATE = "eventDate"
  val LOGDATE = "logDate"
  val DEVICEOBJID = "deviceObjID"
  val DEVCHILDOBJID = "devChildObjID"
  val ITEMNO = "itemNo"
  val SUBITEMNO = "subItemNo"
  val EVENTID = "eventID"
  val PRIORITYID = "priorityID"
  val ISALARM = "isAlarm"
  val SITEID = "siteID"
  val EVENTDESCRIPTION = "eventDescription"
  val DEVICEDESCRIPTION = "deviceDescription"
  val DEVCHILDDESCRIPTION = "devChildDescription"
  val PRIORITYDESCRIPTION = "priorityDescription"
  val ACKOPERATOR = "ackOperator"
  val ACKCOMMENT = "ackComment"
  val ACKDATE = "ackDate"
  val PROCOPERATOR = "procOperator"
  val PROCCOMMENT = "procComment"
  val PROCDATE = "procDate"
  val CATEGORYID = "categoryID"
  val CATEGORYDESCRIPTION = "categoryDescription"
  val CLASSIID = "classiID"
  val CLASSIDESC = "classiDesc"
  val IEVENTDATE = "iEventDate"
  val ILOGDATE = "iLogDate"
  val DEVICETIME = "deviceTime"
  val REVISION = "revision"

  def JSONConvertor(csvReader: CSVReader[Any], url: String) = {
    logger.info("inside JSON Convertor")
    while (csvReader.hasNext) {
      implicit val formats = DefaultFormats
      val jsonString = write(csvReader.next)
      postJSONToES(jsonString, url)

    }
  }


  def postJSONToES(jsonString: String, url: String) = {
    logger.info("Inside postJSONToES")
    val httpClient = HttpClientBuilder.create().build()
    val request = new HttpPost(url)
    val params = new StringEntity(jsonString)
    request.addHeader("content-type", "application/json")
    request.setEntity(params)
    try {
      val response = httpClient.execute(request)
      logger.debug("Successfully Posted " + response.getStatusLine.getStatusCode)
    } catch {
      case e: HttpHostConnectException => logger.error(" Connection failed :" + e)
    }
  }

  def isEmpty(x: String) = x != null && x.nonEmpty

  def assignURL(fileName: String): String = {
    logger.info("Inside assignURL method")

    val stream: InputStream = getClass.getResourceAsStream("/config.properities")

    val lines = scala.io.Source.fromInputStream(stream).getLines.flatMap {
      line =>
        line.split("\\,+")
    }

    while (lines.hasNext) {
      val sequence = lines.next()
      if (sequence.contains(fileName)) {
        val url: String = sequence.concat("").substring(4)
        logger.info("Extracted url is " + url)
        url match {
          case url if isEmpty(url) => return url
          case _ => logger.info("No valid url found")
        }

      }
    }
    ""
  }

  def CSVExtractor(file: File) = {
    logger.info("Inside CSV Extractor")
    val path = file.getAbsolutePath

    val fileName = file.getName

    if (fileName startsWith "FIL") {

      val lineMapper = new LineMapper[FileOutput] {
        override def mapLine(fieldSet: FieldSet): FileOutput = {
          val empCode = fieldSet.readString(EMPCODE).getOrElse("")
          val inDate = fieldSet.readString(INDATE).getOrElse("")
          val outDate = fieldSet.readString(OUTDATE).getOrElse("")
          val totalHours = fieldSet.readString(TOTALHOURS).getOrElse("")
          val siteDescription = fieldSet.readString(SITEDESCRIPTION).getOrElse("")
          val itemDescription = fieldSet.readString(ITEMDESCRIPTION).getOrElse("")
          FileOutput(empCode, inDate, outDate, totalHours, siteDescription, itemDescription)
        }
      }

      val csvReader = new CSVReader[FileOutput](path, ",", List(EMPCODE, INDATE, OUTDATE, TOTALHOURS, SITEDESCRIPTION, ITEMDESCRIPTION), true, lineMapper)

      JSONConvertor(csvReader.asInstanceOf[CSVReader[Any]], assignURL("FIL"))

    } else if (fileName startsWith "Sam") {

      val lineMapper = new LineMapper[SampleEvents] {
        override def mapLine(fieldSet: FieldSet): SampleEvents = {
          val eventNo = fieldSet.readString(EVENTNO).getOrElse("")
          val appType = fieldSet.readString(APPTYPE).getOrElse("")
          val eventDate = fieldSet.readString(EVENTDATE).getOrElse("")
          val logDate = fieldSet.readString(LOGDATE).getOrElse("")
          val deviceObjID = fieldSet.readString(DEVICEOBJID).getOrElse("")
          val devChildObjID = fieldSet.readString(DEVCHILDOBJID).getOrElse("")
          val itemNo = fieldSet.readString(ITEMNO).getOrElse("")
          val subItemNo = fieldSet.readString(SUBITEMNO).getOrElse("")
          val eventID = fieldSet.readString(EVENTID).getOrElse("")
          val priorityID = fieldSet.readString(PRIORITYID).getOrElse("")
          val isAlarm = fieldSet.readString(ISALARM).getOrElse("")
          val siteID = fieldSet.readString(SITEID).getOrElse("")
          val eventDescription = fieldSet.readString(EVENTDESCRIPTION).getOrElse("")
          val siteDescription = fieldSet.readString(SITEDESCRIPTION).getOrElse("")
          val deviceDescription = fieldSet.readString(DEVICEDESCRIPTION).getOrElse("")
          val devChildDescription = fieldSet.readString(DEVCHILDDESCRIPTION).getOrElse("")
          val itemDescription = fieldSet.readString(ITEMDESCRIPTION).getOrElse("")
          val priorityDescription = fieldSet.readString(PRIORITYDESCRIPTION).getOrElse("")
          val ackOperator = fieldSet.readString(ACKOPERATOR).getOrElse("")
          val ackComment = fieldSet.readString(ACKCOMMENT).getOrElse("")
          val ackDate = fieldSet.readString(ACKDATE).getOrElse("")
          val procOperator = fieldSet.readString(PROCOPERATOR).getOrElse("")
          val procComment = fieldSet.readString(PROCCOMMENT).getOrElse("")
          val procDate = fieldSet.readString(PROCDATE).getOrElse("")
          val categoryID = fieldSet.readString(CATEGORYID).getOrElse("")
          val categoryDescription = fieldSet.readString(CATEGORYDESCRIPTION).getOrElse("")
          val classiID = fieldSet.readString(CLASSIID).getOrElse("")
          val classiDesc = fieldSet.readString(CLASSIDESC).getOrElse("")
          val iEventDate = fieldSet.readString(IEVENTDATE).getOrElse("")
          val iLogDate = fieldSet.readString(ILOGDATE).getOrElse("")
          val empCode = fieldSet.readString(EMPCODE).getOrElse("")
          val deviceTime = fieldSet.readString(DEVICETIME).getOrElse("")
          val revision = fieldSet.readString(REVISION).getOrElse("")
          SampleEvents(eventNo, appType, eventDate, logDate, deviceObjID, devChildObjID, itemNo, subItemNo, eventID, priorityID, isAlarm, siteID, eventDescription, siteDescription, deviceDescription, devChildDescription, itemDescription, priorityDescription, ackOperator, ackComment, ackDate, procOperator, procComment, procDate, categoryID, categoryDescription, classiID, classiDesc, iEventDate, iLogDate, empCode, deviceTime, revision)
        }
      }

      val csvReader = new CSVReader[SampleEvents](path, ",", List(EVENTNO, APPTYPE, EVENTDATE, LOGDATE, DEVICEOBJID, DEVCHILDOBJID, ITEMNO, SUBITEMNO, EVENTID, PRIORITYID, ISALARM, SITEID, EVENTDESCRIPTION, SITEDESCRIPTION, DEVICEDESCRIPTION, DEVCHILDDESCRIPTION, ITEMDESCRIPTION, PRIORITYDESCRIPTION, ACKOPERATOR, ACKCOMMENT, ACKDATE, PROCOPERATOR, PROCCOMMENT, PROCDATE, CATEGORYID, CATEGORYDESCRIPTION, CLASSIID, CLASSIDESC, IEVENTDATE, ILOGDATE, EMPCODE, DEVICETIME, REVISION), true, lineMapper)

      JSONConvertor(csvReader.asInstanceOf[CSVReader[Any]], assignURL("SAM"))
    }
  }


}