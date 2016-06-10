package org.imaginea.application.csv

/**
  * Created by charanjits on 9/6/16.
  */


trait LineTokenizer {
  def tokenize(line: String, separator: String): Array[String] =
    line.split(separator).map(value => value.trim())

  def tokenize(line: String, separator: String, labels: List[String]): FieldSet =
    new FieldSet(labels, tokenize(line, separator))
}

trait LineMapper[T] {
  def mapLine(fieldSet: FieldSet): T
}