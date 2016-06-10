package org.imaginea.application.csv

/**
  * Created by charanjits on 9/6/16.
  */

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity

class CSVProcessor extends LazyLogging {

  case class SampleEvents(EventNo: String, AppType: String, EventDate: String, LogDate: String, DeviceObjID: String, DevChildObjID: String, ItemNo: String, SubItemNo: String, EventID: String, PriorityID: String, IsAlarm: String, SiteID: String, EventDescription: String, SiteDescription: String, DeviceDescription: String, DevChildDescription: String, ItemDescription: String, PriorityDescription: String, AckOperator: String, AckComment: String, AckDate: String, ProcOperator: String, ProcComment: String, ProcDate: String, CategoryID: String, CategoryDescription: String, ClassiId: String, ClassiDesc: String, iEventDate: String, iLogDate: String, empCode: String, DeviceTime: String, revision: String)

  case class FileOutput(Empcode: String, InDate: String, OutDate: String, TotalHours: String, sitedescription: String, ItemDescription: String)

  val EMPCODE = "Empcode"
  val INDATE = "InDate"
  val OUTDATE = "OutDate"
  val TOTALHOURS = "TotalHours"
  val SITEDESCRIPTION = "sitedescription"
  val ITEMDESCRIPTION = "ItemDescription"


  val EVENTNO = "EventNo"
  val APPTYPE = "AppType"
  val EVENTDATE = "EventDate"
  val LOGDATE = "LogDate"
  val DEVICEOBJID = "DeviceObjID"
  val DEVCHILDOBJID = "DevChildObjID"
  val ITEMNO = "ItemNo"
  val SUBITEMNO = "SubItemNo"
  val EVENTID = "EventID"
  val PRIORITYID = "PriorityID"
  val ISALARM = "IsAlarm"
  val SITEID = "SiteID"
  val EVENTDESCRIPTION = "EventDescription"
  // val SITEDESCRIPTION="SiteDescription"
  val DEVICEDESCRIPTION = "DeviceDescription"
  val DEVCHILDDESCRIPTION = "DevChildDescription"
  //val ITEMDESCRIPTION="ItemDescription"
  val PRIORITYDESCRIPTION = "PriorityDescription"
  val ACKOPERATOR = "AckOperator"
  val ACKCOMMENT = "AckComment"
  val ACKDATE = "AckDate"
  val PROCOPERATOR = "ProcOperator"
  val PROCCOMMENT = "ProcComment"
  val PROCDATE = "ProcDate"
  val CATEGORYID = "CategoryID"
  val CATEGORYDESCRIPTION = "CategoryDescription"
  val CLASSIID = "ClassiId"
  val CLASSIDESC = "ClassiDesc"
  val IEVENTDATE = "iEventDate"
  val ILOGDATE = "iLogDate"
  //val EMPCODE="empCode"
  val DEVICETIME = "DeviceTime"
  val REVISION = "revision"

  def JSONConvertor(csvReader: CSVReader[Any],url:String) = {
    logger.info("inside JSON Convertor")
    while (csvReader.hasNext) {
      implicit val formats = DefaultFormats
      val jsonString = write(csvReader.next)
      postJSONToES(jsonString,url)  //http://localhost:9200/filo1/profile/
      //println(jsonString)
    }
  }


  def postJSONToES(jsonString: String,url:String) = {
    val httpClient = HttpClientBuilder.create().build();
    val request = new HttpPost(url);
    val params = new StringEntity(jsonString);
    request.addHeader("content-type", "application/json");
    request.setEntity(params);
    httpClient.execute(request);

  }


  def CSVExtractor(file: File) = {
    logger.info("Inside CSV Extractor")
    val path = file.getAbsolutePath
    val url:String=""

    if (file.getName() startsWith ("FIL")) {

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

      JSONConvertor(csvReader.asInstanceOf[CSVReader[Any]],url)

    } else {

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

      JSONConvertor(csvReader.asInstanceOf[CSVReader[Any]],url)
    }
  }


}