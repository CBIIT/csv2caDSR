package org.renci.ccdh.csv2cadsr

import org.json4s.{DefaultFormats}

import scala.io.Source
import org.json4s.native.Serialization.writePretty

object csv2caDSR extends App {
  val csvFilename: String = args(0)
  val csvSource: Source = Source.fromFile(csvFilename)("UTF-8")

  implicit val formats = DefaultFormats

  val result = schema.MappingGenerator.generateFromCsv(csvSource)
  result.fold(
    throwable => scribe.error(s"Could not generate CSV: ${throwable}"),
    result => println(writePretty(result.asJsonSchema))
  )
}
