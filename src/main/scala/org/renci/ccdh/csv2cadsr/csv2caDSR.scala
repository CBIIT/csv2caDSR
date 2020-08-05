package org.renci.ccdh.csv2cadsr

import org.json4s.{DefaultFormats, StringInput}

import scala.io.Source
import org.json4s.native.Serialization.writePretty
import org.json4s.native.JsonMethods._

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
  } else if(csvFilename == "fill") {
    // This is a hack to fill in the JSON Schema with information from the caDSR system.
    val jsonSource: Source = Source.fromFile(jsonFilename.get)("UTF-8")

    val filledScheme = schema.Filler.fill(parse(StringInput(jsonSource.getLines().mkString("\n"))))
    println(writePretty(filledScheme))
  } else {
    // We have a JSON schema file and a CSV file. Generate the JSON!
    val csvSource: Source = Source.fromFile(csvFilename)("UTF-8")
    val jsonSource: Source = Source.fromFile(jsonFilename.get)("UTF-8")
  }
}
