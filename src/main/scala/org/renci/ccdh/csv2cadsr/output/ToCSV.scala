package org.renci.ccdh.csv2cadsr.output

import java.io.BufferedWriter

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import org.json4s.JValue

import scala.collection.immutable.HashMap

/**
  * Export harmonized data as a CSV file with annotations.
  */
object ToCSV {
  /**
    * Write out the CSV data as a harmonized CSV file.
    *
    * @param reader A CSV reader for the input data.
    * @param mappedProperties The mapped properties from the JSON mapping file.
    * @param writer A writer to write out the CSV file to.
    */
  def write(reader: CSVReader, mappedProperties: Map[String, JValue], writer: BufferedWriter): Unit = {
    // Read the input data.
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // Write out the header row for the CSV data.
    val csvWriter = CSVWriter.open(writer)
    csvWriter.writeRow(headerRow flatMap { rowName =>
      val caDSR = {
        val property = mappedProperties.getOrElse(rowName, HashMap()).asInstanceOf[Map[String, String]]
        val caDSR = property.getOrElse("caDSR", "")
        val caDSRVersion = property.getOrElse("caDSRVersion", "")
        if (caDSR.nonEmpty && caDSRVersion.nonEmpty) s"${caDSR}v$caDSRVersion"
        else caDSR
      }
      Seq(rowName, s"${rowName}_caDSR_cde_${caDSR}_value", s"${rowName}_ncit_uri")
    })

    // Write out the CSV data, adding mapped values where possible.
    dataWithHeaders.foreach(row => {
      val rowValues: Seq[String] = headerRow flatMap { rowName =>
        val rowValue = row.getOrElse(rowName, "")

        val rowProp = mappedProperties.getOrElse(rowName, HashMap()).asInstanceOf[Map[String, _]]
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
