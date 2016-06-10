package org.imaginea.application.csv

/**
  * Created by charanjits on 9/6/16.
  */

import java.lang.Boolean

import scala.io.Source

class CSVReader[T](file: String, separator: String, labels: List[String], hasHeader: Boolean,
                   lineMapper: LineMapper[T]) extends LineTokenizer {
  val lines = Source.fromFile(file).getLines()
  val header = if (hasHeader) lines.drop(2) else ""

  def hasNext: Boolean = lines.hasNext

  def next: T = lineMapper.mapLine(tokenize(lines.next(), separator, labels))
}
