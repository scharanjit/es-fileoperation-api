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
    val httpClient = HttpClientBuilder.create().build()
    val request = new HttpPost(url)
    val params = new StringEntity(jsonString)
    request.addHeader("content-type", "application/json")
    request.setEntity(params)
    httpClient.execute(request)

  }

  def isEmpty(x: String) = x != null && x.nonEmpty

  def assignURL(fileName: String): String = {
    logger.info("Inside assignURL method")

    val stream: InputStream = getClass.getResourceAsStream("/config.properities")
    val lines = scala.io.Source.fromInputStream(stream).getLines.flatMap { line =>
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
          val Empcode = fieldSet.readString(EMPCODE).getOrElse("")
          val InDate = fieldSet.readString(INDATE).getOrElse("")
          val OutDate = fieldSet.readString(OUTDATE).getOrElse("")
          val TotalHours = fieldSet.readString(TOTALHOURS).getOrElse("")
          val sitedescription = fieldSet.readString(SITEDESCRIPTION).getOrElse("")
          val ItemDescription = fieldSet.readString(ITEMDESCRIPTION).getOrElse("")
          FileOutput(Empcode, InDate, OutDate, TotalHours, sitedescription, ItemDescription)
        }
      }

      val csvReader = new CSVReader[FileOutput](path, ",", List(EMPCODE, INDATE, OUTDATE, TOTALHOURS, SITEDESCRIPTION, ITEMDESCRIPTION), true, lineMapper)

      JSONConvertor(csvReader.asInstanceOf[CSVReader[Any]], assignURL("FIL"))

    } else if (fileName startsWith "Sam") {

      val lineMapper = new LineMapper[SampleEvents] {
        override def mapLine(fieldSet: FieldSet): SampleEvents = {
          val EventNo = fieldSet.readString(EVENTNO).getOrElse("")
          val AppType = fieldSet.readString(APPTYPE).getOrElse("")
          val EventDate = fieldSet.readString(EVENTDATE).getOrElse("")
          val LogDate = fieldSet.readString(LOGDATE).getOrElse("")
          val DeviceObjID = fieldSet.readString(DEVICEOBJID).getOrElse("")
          val DevChildObjID = fieldSet.readString(DEVCHILDOBJID).getOrElse("")
          val ItemNo = fieldSet.readString(ITEMNO).getOrElse("")
          val SubItemNo = fieldSet.readString(SUBITEMNO).getOrElse("")
          val EventID = fieldSet.readString(EVENTID).getOrElse("")
          val PriorityID = fieldSet.readString(PRIORITYID).getOrElse("")
          val IsAlarm = fieldSet.readString(ISALARM).getOrElse("")
          val SiteID = fieldSet.readString(SITEID).getOrElse("")
          val EventDescription = fieldSet.readString(EVENTDESCRIPTION).getOrElse("")
          val SiteDescription = fieldSet.readString(SITEDESCRIPTION).getOrElse("")
          val DeviceDescription = fieldSet.readString(DEVICEDESCRIPTION).getOrElse("")
          val DevChildDescription = fieldSet.readString(DEVCHILDDESCRIPTION).getOrElse("")
          val ItemDescription = fieldSet.readString(ITEMDESCRIPTION).getOrElse("")
          val PriorityDescription = fieldSet.readString(PRIORITYDESCRIPTION).getOrElse("")
          val AckOperator = fieldSet.readString(ACKOPERATOR).getOrElse("")
          val AckComment = fieldSet.readString(ACKCOMMENT).getOrElse("")
          val AckDate = fieldSet.readString(ACKDATE).getOrElse("")
          val ProcOperator = fieldSet.readString(PROCOPERATOR).getOrElse("")
          val ProcComment = fieldSet.readString(PROCCOMMENT).getOrElse("")
          val ProcDate = fieldSet.readString(PROCDATE).getOrElse("")
          val CategoryID = fieldSet.readString(CATEGORYID).getOrElse("")
          val CategoryDescription = fieldSet.readString(CATEGORYDESCRIPTION).getOrElse("")
          val ClassiId = fieldSet.readString(CLASSIID).getOrElse("")
          val ClassiDesc = fieldSet.readString(CLASSIDESC).getOrElse("")
          val iEventDate = fieldSet.readString(IEVENTDATE).getOrElse("")
          val iLogDate = fieldSet.readString(ILOGDATE).getOrElse("")
          val empCode = fieldSet.readString(EMPCODE).getOrElse("")
          val DeviceTime = fieldSet.readString(DEVICETIME).getOrElse("")
          val revision = fieldSet.readString(REVISION).getOrElse("")
          SampleEvents(EventNo, AppType, EventDate, LogDate, DeviceObjID, DevChildObjID, ItemNo, SubItemNo, EventID, PriorityID, IsAlarm, SiteID, EventDescription, SiteDescription, DeviceDescription, DevChildDescription, ItemDescription, PriorityDescription, AckOperator, AckComment, AckDate, ProcOperator, ProcComment, ProcDate, CategoryID, CategoryDescription, ClassiId, ClassiDesc, iEventDate, iLogDate, empCode, DeviceTime, revision)
        }
      }

      val csvReader = new CSVReader[SampleEvents](path, ",", List(EVENTNO, APPTYPE, EVENTDATE, LOGDATE, DEVICEOBJID, DEVCHILDOBJID, ITEMNO, SUBITEMNO, EVENTID, PRIORITYID, ISALARM, SITEID, EVENTDESCRIPTION, SITEDESCRIPTION, DEVICEDESCRIPTION, DEVCHILDDESCRIPTION, ITEMDESCRIPTION, PRIORITYDESCRIPTION, ACKOPERATOR, ACKCOMMENT, ACKDATE, PROCOPERATOR, PROCCOMMENT, PROCDATE, CATEGORYID, CATEGORYDESCRIPTION, CLASSIID, CLASSIDESC, IEVENTDATE, ILOGDATE, EMPCODE, DEVICETIME, REVISION), true, lineMapper)

      JSONConvertor(csvReader.asInstanceOf[CSVReader[Any]], assignURL("SAM"))
    }
  }


}