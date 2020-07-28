package org.renci.ccdh.csv2cadsr.schema

import scala.io.Source
import scala.util.{Success, Try}
import com.github.tototoshi.csv._

/**
 * This object contains methods for generating MappingSchema with different methods.
 */
object MappingGenerator {
  def generateFromCsv(csvSource: Source): Try[MappingSchema] = {
    // Step 1. Load data from CSV file.
    val reader = CSVReader.open(csvSource)
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // Headers are in the first row.
    val fields = headerRow map { fieldName => MappingField.createFromValues(
      fieldName,
      dataWithHeaders.flatMap(_.get(fieldName))
    )}

    Success(MappingSchema(fields))
  }
}
