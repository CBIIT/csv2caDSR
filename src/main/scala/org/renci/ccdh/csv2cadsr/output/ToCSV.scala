package org.renci.ccdh.csv2cadsr.output

import java.io.Writer

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import org.json4s.JValue

import scala.collection.immutable.HashMap

/**
  * Converts a CSV file to another CSV file with annotations.
  */
object ToCSV extends CSVToOutput {
  override def write(reader: CSVReader, properties: Map[String, JValue], writer: Writer): Unit = {
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // For now, we write to STDOUT.
    val csvWriter = CSVWriter.open(writer)
    csvWriter.writeRow(headerRow flatMap { rowName =>
      val caDSR = {
        val property = properties.getOrElse(rowName, HashMap()).asInstanceOf[Map[String, String]]
        val caDSR = property.getOrElse("caDSR", "")
        val caDSRVersion = property.getOrElse("caDSRVersion", "")
        if (caDSR.nonEmpty && caDSRVersion.nonEmpty) s"${caDSR}v$caDSRVersion"
        else caDSR
      }
      Seq(rowName, s"${rowName}_caDSR_cde_${caDSR}_value", s"${rowName}_ncit_uri")
    })

    dataWithHeaders.foreach(row => {
      val rowValues: Seq[String] = headerRow flatMap { rowName =>
        val rowValue = row.getOrElse(rowName, "")

        val rowProp = properties.getOrElse(rowName, HashMap()).asInstanceOf[Map[String, _]]
        val enumValues =
          rowProp.getOrElse("enumValues", List()).asInstanceOf[List[Map[String, String]]]
        val mapping: Map[String, String] =
          enumValues.find(_.getOrElse("value", "") == rowValue).getOrElse(HashMap())
        val caDSRValue = mapping.getOrElse("caDSRValue", "")
        val conceptURI = mapping.getOrElse("conceptURI", "")

        Seq(rowValue, caDSRValue, conceptURI)
      }
      csvWriter.writeRow(rowValues)
    })

    csvWriter.close()
  }
}
