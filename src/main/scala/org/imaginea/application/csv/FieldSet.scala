package org.imaginea.application.csv

/**
  * Created by charanjits on 9/6/16.
  */

import scala.util.Try
import java.lang.Boolean


class FieldSet(labels: List[String], values: Array[String]) {
  val fields = labels.zip(values).toMap

  def readString(field: String): Option[String] = fields.get(field)

  def readBoolean(field: String): Option[Boolean] =
    convert(field, (s: String) => Boolean.valueOf(s))

  def readInt(field: String): Option[Int] = convert(field, (s: String) => s.toInt)

  def readLong(field: String): Option[Long] = convert(field, (s: String) => s.toLong)

  def readBigDecimal(field: String): Option[BigDecimal] =
    convert(field, (s: String) => BigDecimal.valueOf(s.toDouble))

  def convert[T](field: String, f: String => T): Option[T] = {
    readString(field) match {
      case None => None
      case Some(x) => Try[Option[T]](Some(f(x))).getOrElse(None)
    }
  }

  override def toString =
    List("FieldSet([", labels.mkString(","), "],[", values.mkString(","), "])").mkString
}