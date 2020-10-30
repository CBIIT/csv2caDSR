package org.renci.ccdh.csv2cadsr.output

import java.io.{File, Writer}
import java.nio.charset.Charset
import java.util.Collections

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import org.json4s.JValue
import org.apache.avro._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.tools.nsc.interpreter.shell.WriterOutputStream

/**
  * Converts a CSV file to a PFB file with annotations.
  */
object ToPFB extends CSVToOutput {
  override def write(reader: CSVReader, properties: Map[String, JValue], outputWriter: Writer): Unit = {
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // Step 1. Create a schema for the output.
    val schemaBuilder = SchemaBuilder
      .record("export") // TODO: name this after the input filename, I guess?
    var fieldsBuilder = schemaBuilder.fields()

    // TODO: this should be colName, not rowName.
    val fieldTypes = mutable.Map[String, String]()
    headerRow foreach { rowName =>
      val property = properties.getOrElse(rowName, HashMap()).asInstanceOf[Map[String, String]]

      val caDSR = {
        val caDSR = property.getOrElse("caDSR", "")
        val caDSRVersion = property.getOrElse("caDSRVersion", "")
        if (caDSR.nonEmpty && caDSRVersion.nonEmpty) s"${caDSR}v$caDSRVersion"
        else caDSR
      }
      val fieldType = property.getOrElse("type", "string") match {
        case "integer" => "long"  // We could also use "int" here if we want 32 bit ints.
        case "number" => "string" // We'll store it as a string and let it be reparsed (hopefully into BigDecimal) at the other end.
        case str => str
      }
      fieldTypes.put(rowName, fieldType)

      fieldsBuilder = fieldsBuilder
        .name(rowName.replaceAll("\\W","_"))
        .`type`(Schema.createUnion(
          Schema.create(Schema.Type.NULL),
          Schema.create(Schema.Type.valueOf(fieldType.toUpperCase))
        ))
        .noDefault()
    }

    val exportSchema = fieldsBuilder.endRecord()
    val schema = pfb.PFBSchemas.generatePFBForSchemas(Seq(exportSchema))
    scribe.info(s"Created schema: ${schema.toString(true) }")

    // TODO: write out a Metadata entity that describes this object.
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(schema, new File("output.avro"))

    dataWithHeaders.zipWithIndex.foreach({ case (row, index) =>
      val export = new GenericData.Record(exportSchema)
      row.keys.map({ colName =>
        val value = row.getOrElse(colName, "")
        val fieldType = fieldTypes.getOrElse(colName, "string")
        val convertedValue = fieldType match {
          case "long" => if (value == "") null else value.toLong
          case _ => value
        }

        export.put(
          colName.replaceAll("\\W","_"),
          convertedValue
        )
      })

      val datum = new GenericData.Record(schema)
      datum.put("id", "row" + index)
      datum.put("name", "export")
      datum.put("object", export)
      datum.put("relations", Collections.EMPTY_LIST)

      dataFileWriter.append(datum)
    })

    dataFileWriter.close()

    // For now, we write to STDOUT.
    /*
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
     */
  }
}
