package org.renci.ccdh.csv2cadsr.output

import java.io.Writer

import com.github.tototoshi.csv.CSVReader
import org.json4s.JValue

/**
  * The overall trait for all outputers.
  */
trait CSVToOutput {
  def write(reader: CSVReader, properties: Map[String, JValue], writer: Writer)
}
