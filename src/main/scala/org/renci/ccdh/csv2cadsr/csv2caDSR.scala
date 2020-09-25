package org.renci.ccdh.csv2cadsr

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import org.json4s.{DefaultFormats, JObject, JValue, StringInput}

import scala.io.Source
import org.json4s.native.Serialization.writePretty
import org.json4s.native.JsonMethods._

import scala.collection.immutable.HashMap

object csv2caDSR extends App {
  val csvFilename: String = args(0)
  val jsonFilename: Option[String] = args.tail.headOption

  implicit val formats = DefaultFormats

  if (jsonFilename.isEmpty) {
    // No JSON file provided? Then generate the JSON file!
    val csvSource: Source = Source.fromFile(csvFilename)("UTF-8")

    val result = schema.MappingGenerator.generateFromCsv(csvSource)
    result.fold(
      throwable => scribe.error(s"Could not generate JSON Schema: ${throwable}"),
      result => println(writePretty(result.asJsonSchema))
    )
  } else if (csvFilename == "fill") {
    // This is a hack to fill in the JSON Schema with information from the caDSR system.
    val jsonSource: Source = Source.fromFile(jsonFilename.get)("UTF-8")

    val filledScheme = schema.Filler.fill(parse(StringInput(jsonSource.getLines().mkString("\n"))))
    println(writePretty(filledScheme))
  } else {
    // We have a JSON schema file and a CSV file. Generate the JSON!
    val csvSource: Source = Source.fromFile(csvFilename)("UTF-8")
    val jsonSource: Source = Source.fromFile(jsonFilename.get)("UTF-8")

    val jsonRoot = parse(StringInput(jsonSource.getLines().mkString("\n")))
    val properties: Map[String, JValue] = jsonRoot match {
      case obj: JObject =>
        obj.values.getOrElse("properties", HashMap()).asInstanceOf[HashMap[String, JValue]]
      case _ => throw new RuntimeException("JSON source is not a JSON object")
    }

    val reader = CSVReader.open(csvSource)
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // For now, we write to STDOUT.
    val writer = CSVWriter.open(System.out)
    writer.writeRow(headerRow flatMap { rowName =>
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
      writer.writeRow(rowValues)
    })

    writer.close()
  }
}
