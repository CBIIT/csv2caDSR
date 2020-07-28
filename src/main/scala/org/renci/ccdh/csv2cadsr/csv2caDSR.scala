package org.renci.ccdh.csv2cadsr

import scala.io.Source

object csv2caDSR extends App {
  val csvFilename: String = args(0)
  val csvSource: Source = Source.fromFile(csvFilename)("UTF-8")

  println(schema.MappingGenerator.generateFromCsv(csvSource))
}
