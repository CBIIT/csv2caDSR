package org.renci.ccdh.csv2cadsr.output

import java.io.{BufferedWriter, File, FileWriter}

import com.github.tototoshi.csv.CSVReader
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, JArray, JField, JNothing, JObject, JString}

import scala.collection.mutable

/**
  * Converts a harmonized CSV file to JSON-LD instance data, along with validation information.
  *
  * Only SHACL is currently supported, but ShEx and JSON Schema could be added if needed.
  *
  * @param uriPrefix The URI prefix to use when generating URIs.
  */
class ToJSONLD(
  uriPrefix: String = "http://example.org/csv2cadsr#"
 ) {
  // We use default formats to read values from JSON objects.
  implicit val formats = DefaultFormats

  /** We use EnumMapping to wrap the mapping of values to concept URIs, along with other information about the column
    * they're from, descriptions, caDSR values and so on. */
  case class EnumMapping(
    /** The column name that this mapping is part of. */
    colName: String,
    /** The column object that this mapping is part of. */
    colObject: JObject,
    /** The verbatim value of this enum mapping in the data file. */
    value: String,
    /** The description of this enum mapping from the JSON mapping file. */
    description: Option[String],
    /** The value of this enum mapping in the CDE entry. */
    caDSRValue: Option[String],
    /** The concept URI of this enum mapping as per the CDE entry. */
    conceptURI: Option[String]
  )

  /**
    * Get a URI to the CDE from the CDE ID and the caDSR Version.
    */
  def getCDEURI(props: JObject): Option[String] = {
    (props \ "caDSR") match {
      case JString(cdeId) if cdeId.nonEmpty => {
        // Build an URI using the cdeId
        val cdeVersion = (props \ "caDSRVersion") match {
          case JString(cdeVersion) if cdeVersion.nonEmpty => cdeVersion
          case _ => "1.0" // Default to 1.0
        }

        Some(s"https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#/search?publicId=${cdeId}&version=${cdeVersion}")
      }
      case _ => {
        // If we can't figure this out, give up.
        None
      }
    }
  }

  /**
    * Return a URI representing a column name. We simply replace any invalid characters with '_'. Note that we don't
    * check for duplicates, since column names shouldn't be duplicated to begin with.
    *
    * @param colName The column name to convert into a URI.
    * @return A URI representing this column name.
    */
  def getURIForColumn(colName: String): String = {
    uriPrefix + colName.replaceAll("[^A-Za-z0-9\\-._~()'!*:@,;]", "_")
  }

  /**
    * Write JSON-LD data (and optionally SHACL validation information) for CSV data.
    *
    * @param inputCSVFile The input CSV file to be exported to JSON-LD. We use this to create URIs for rows.
    * @param reader A CSVReader used to read the input CSV file.
    * @param properties Properties for describing the columns as recorded in the JSON mapping file.
    * @param requiredPropNames A set of property names that are required.
    * @param outputPrefix The prefix used to generate output data. We generate `${outputPrefix}.shacl.ttl` for the SHACL
    *                     template and `${outputPrefix}.instance.${index}.jsonld`, with index starting from zero.
    * @param generateSHACL Should we generate a SHACL template?
    * @param generateJSONSchema Should we generate a JSON Schema? (Not currently implemented)
    */
  def writeJSONLD(
    inputCSVFile: File,
    reader: CSVReader,
    properties: Map[String, JValue],
    requiredPropNames: Set[String],
    outputPrefix: String,
    generateSHACL: Boolean,
    generateJSONSchema: Boolean
  ): Unit = {
    // Transform data files into JSON-LD files.
    // This pretty much just means that:
    //  - For each field, we need to map it to an RDF property.
    //  - For each value, we need to specify its type.
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // Step 1. Create @context.
    // We could create a more complex @context, but for now this is good enough for testing.
    val basicContext: JObject =
      ("rdfs" -> "http://www.w3.org/2000/01/rdf-schema#") ~
      ("rdf" -> "http://www.w3.org/1999/02/22-rdf-syntax-ns#") ~
      ("xsd" -> "http://www.w3.org/2001/XMLSchema#") ~
      ("obo" -> "http://purl.obolibrary.org/obo/") ~
      ("ncit" -> "http://purl.obolibrary.org/obo/NCIT_") ~
      ("ncicde" -> "https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#") ~
      ("example" -> uriPrefix)

    val propertyTypesMap = mutable.Map[String, String]()
    val contextProperties = JObject(headerRow map { colName =>
      val colURI = properties.get(colName) match {
        case Some(prop: JObject) => {
          (prop \ "@type").toOption match {
            case Some(JString(typeName)) => propertyTypesMap.put(colName, typeName)
            case _ =>
          }

          getURIForColumn(colName)
        }
        case Some(unk) =>
          throw new RuntimeException(s"Unknown object in property information: $unk.")
        case None =>
          throw new RuntimeException(s"Property $colName not present in JSON mapping file.")
        case unk => throw new RuntimeException(s"Property $colName has unexpected object: $unk.")
      }

      JField(colName, ("@id" -> colURI))
    })

    val context = basicContext ~ contextProperties

    // Step 1. Generate the SHACL file.
    if (generateSHACL) {
      // We generate outputPrefix + ".shacl.ttl"
      val shaclFilename = new File(outputPrefix + s".shacl.ttl")

      // Write out descriptions for each property.
      val allEnumMappings = mutable.ListBuffer[EnumMapping]()
      val propertyShapes = headerRow map { colName =>
        properties.get(colName) match {
          case Some(prop: JObject) => {
            val propType = (prop \ "enumValues") match {
              case JArray(enumValues) if enumValues.nonEmpty =>
                val enumMappings = enumValues.map({
                  case obj: JObject =>
                    EnumMapping(
                      colName,
                      prop,
                      (obj \ "value").extractOrElse[String](""),
                      (obj \ "description").extractOpt[String],
                      (obj \ "caDSRValue").extractOpt[String],
                      (obj \ "conceptURI").extractOpt[String]
                    )
                  case unk => throw new RuntimeException(s"Expected object as enumValue, but found $unk")
                })
                allEnumMappings.appendAll(enumMappings)

                // Note down which CDE this field is mapped to.
                val cdeMapping = enumMappings.flatMap(enumMapping => getCDEURI(enumMapping.colObject)).distinct.map(uri => s"example:fromCDE <$uri> ;")

                // Concepts with and without IDs need to be handled separately.
                val conceptsWithIDs = enumMappings.filter(_.conceptURI.nonEmpty)
                val conceptsWithoutIDs = enumMappings.filter(_.conceptURI.isEmpty)

                // Given two concepts with IDs (say, ncit:A1 and ncit:A2, although any URI can be used) and two
                // concepts without IDs (say, 'Yellow' and 'Red'), then the SHACL expression we create is in the form:
                //  sh:or (
                //    [ sh:in ( ncit:A1 ncit:A2 ... ) ]
                //    [ sh:value "Yellow"^^<xsd:string> ]
                //    [ sh:value "Red"^^<xsd:string> ]
                //  )
                val sh_in = "[ sh:in (" + conceptsWithIDs.flatMap(_.conceptURI).map(uri => s"<$uri>").mkString(" ") + ") ]"
                val sh_values = conceptsWithoutIDs.map(mapping => mapping.caDSRValue.getOrElse(mapping.value))
                  .map(_.prepended('"').appended('"'))
                  .map(value => s"[ sh:value $value^^<xsd:string> ]")
                  .mkString("\n      ")

                s"""
                   |    ${cdeMapping.mkString("\n    ")}
                   |    sh:nodeKind sh:IRIOrLiteral ;
                   |    sh:or (
                   |      ${ if(conceptsWithIDs.nonEmpty) sh_in else "" }
                   |      ${sh_values}
                   |    )
                   |""".stripMargin

              case _ =>
                (prop \ "type") match {
                  case JString("integer") =>
                    s"""
                       |    sh:nodeKind sh:Literal ;
                       |    xsd:dataType xsd:integer ;
                       |""".stripMargin
                  case JString("number") =>
                    s"""
                       |    sh:nodeKind sh:Literal ;
                       |    xsd:dataType xsd:decimal ;
                       |""".stripMargin
                  case JString(_) =>
                    s"""
                       |    sh:nodeKind sh:Literal ;
                       |    xsd:dataType xsd:string ;
                       |""".stripMargin
                  case unk => throw new RuntimeException(s"Expected the type described as a string but found $unk.")
                }
            }

            // Calculate cardinality. For now, we assume that maxCount is always 1.
            val minCount = if(requiredPropNames.contains(colName)) 1 else 0

            s"""
               |  sh:property [
               |    sh:name "$colName" ;
               |    sh:description "${(prop \ "description").extractOrElse[String]("")}" ;
               |    sh:path <${getURIForColumn(colName)}> ;
               |
               |    sh:minCount $minCount ;
               |    sh:maxCount 1 ;
               |$propType
               |  ] ;
               |""".stripMargin
          }
          case Some(unk) =>
            throw new RuntimeException(s"Unknown object in property information: $unk.")
          case None =>
            throw new RuntimeException(s"Property $colName not present in JSON mapping file.")
          case unk => throw new RuntimeException(s"Property $colName has unexpected object: $unk.")
        }
      }

      // Add descriptions for concepts.
      val enumMappingsAsDescriptions = allEnumMappings
        .distinct
        .filter (_.conceptURI.nonEmpty)
        .map { enumMapping =>
          val cdeMapping = getCDEURI(enumMapping.colObject).map(uri => s"dc:source <$uri>")
          s"""
            |<${enumMapping.conceptURI.get}>
            |  rdfs:label "${enumMapping.caDSRValue.getOrElse("")}" ;
            |  dc:description "${enumMapping.description.getOrElse("")}" ;
            |  ${cdeMapping.mkString("")}
            |.
            |""".stripMargin
        }

      // Write out the SHACL file.
      val shaclWriter = new BufferedWriter(new FileWriter(shaclFilename))
      shaclWriter.write(
        s"""
           |@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
           |@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
           |@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
           |@prefix dc: <http://purl.org/dc/elements/1.1/> .
           |@prefix sh: <http://www.w3.org/ns/shacl#> .
           |@prefix example: <$uriPrefix> .
           |
           |example:ExportSchema a sh:NodeShape ;
           |  sh:targetClass example:ExportSchema ;
           |  ${propertyShapes.mkString("")}
           |.
           |
           |# Descriptions for values in caDSR CDEs.
           |${enumMappingsAsDescriptions.mkString("")}
           |""".stripMargin)

      shaclWriter.close()
      scribe.info(s"Wrote out SHACL shapes to $shaclFilename")
    }

    // Step 2. Generate a JSON Schema.
    if (generateJSONSchema) {
      // TODO: we will generate outputPrefix + ".schema.json"
      ???
    }

    // Step 3. Create a JSON-LD Instance for each row in the input data.
    dataWithHeaders.zipWithIndex
      .foreach({
        case (row, index) =>
          val values: Seq[JField] = headerRow
            .map(colName => (colName, row.get(colName)))
            .map({
              case (colName, Some(value)) => {
                val prop = properties.getOrElse(colName, JObject(List()))

                val mappings: Seq[JObject] = (prop \ "enumValues") match {
                  case JArray(enumValues) => {
                    enumValues flatMap { enumValue =>
                      val mappingValue = (enumValue \ "value").extract[String]
                      // scribe.info(s"Comparing '$value' with '$mappingValue'")
                      if (value == mappingValue) {
                        // Got a map!
                        val uri = (enumValue \ "conceptURI").extract[String]
                        // val caDSRValue = (enumValue \ "caDSRValue").extract[String]

                        if (uri.isEmpty) Some(
                          // We get here if one of the values was mapped to a caDSR value which doesn't have a concept URI.
                          ("@value" -> mappingValue) ~
                          ("@type" -> "xsd:string")
                        ) else Some(
                            ("@id" -> uri)
                          )
                      } else {
                        None
                      }
                    }
                  }
                  case JNothing => Seq()
                  case unk =>
                    throw new RuntimeException(
                      s"enumValues in property $colName is an unexpected value: $unk."
                    )
                }

                // Create a JSON field whose label is the name of the column name and whose value is a representation
                // of its value.
                JField(
                  colName,
                  if (mappings.nonEmpty) {
                    // We found mappings!
                    if (mappings.length > 1)
                      throw new RuntimeException(s"Too many mappings found for $colName: $mappings")
                    mappings.head
                  } else {
                    // No enum values? Check if it's a numerical type.
                    val numberType = (prop \ "type") match {
                      case JString("integer") =>
                        Some("xsd:long") // We can also use xsd:int if we want short ints.
                      case JString("number") =>
                        Some("xsd:decimal")
                      case _                 => None
                    }

                    if (numberType.nonEmpty) {
                      ("@value" -> value) ~
                        ("@type" -> numberType.get)
                    } else {
                      ("@value" -> value) ~
                        ("@type" -> "xsd:string")
                    }
                  }
                )
              }
              case (colName, None) => JField(colName, "")
            })

          // Generate an instance object with all the property values.
          val instance =
            ("@id" -> s"${inputCSVFile.toURI}#row$index") ~
            ("@type" -> "example:ExportSchema") ~
            ("@context" -> context) ~
            JObject(values.toList)

          // Write out the instance value to a JSON-LD file.
          val instanceFilename = new File(outputPrefix + s".instance.$index.jsonld")
          val instanceWriter = new FileWriter(instanceFilename)
          instanceWriter.append(pretty(render(instance)))
          instanceWriter.close()
          scribe.info(s"Wrote out row $index to $instanceFilename")

        case unk => throw new RuntimeException(s"Unexpected value in JSON mappings file: ${unk}")
      })
  }
}
