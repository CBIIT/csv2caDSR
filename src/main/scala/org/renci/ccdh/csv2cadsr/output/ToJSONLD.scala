package org.renci.ccdh.csv2cadsr.output

import java.io.{BufferedWriter, File, FileWriter}

import com.github.tototoshi.csv.CSVReader
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.native.JsonMethods._
import org.json4s.{JArray, JField, JNothing, JObject, JString}

import scala.collection.mutable

/**
  * Converts a harmonized CSV file to JSON-LD instance data, whether with or without a JSON Schema.
  */
object ToJSONLD {
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
    * Extract a property represented by a string from a JSON object and return it as a string.
    */
  def extractJString(jobj: JObject, propName: String): Option[String] = {
    (jobj \ propName) match {
      case JString(str) if str.isEmpty => None
      case JString(str) => Some(str)
      case unk => throw new RuntimeException(s"Expected property '$propName' in JSON object $jobj to be a string, but found: $unk.")
    }
  }

  /**
    * Get a link to the CDE.
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
        // If we can't figure this out, let's build a dummy URI from the colName.
        // As per https://stackoverflow.com/a/40415059/27310
        None
      }
    }
  }

  def getURIForColumn(colName: String): String = {
    "example:" + colName.replaceAll("[^A-Za-z0-9\\-._~()'!*:@,;]", "_")
  }

  def writeJSONLD(
    inputFile: File,
    reader: CSVReader,
    properties: Map[String, JValue],
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
    val basicContext: JObject =
      ("rdfs" -> "http://www.w3.org/2000/01/rdf-schema#") ~
      ("rdf" -> "http://www.w3.org/1999/02/22-rdf-syntax-ns#") ~
      ("ncit" -> "http://purl.obolibrary.org/obo/NCIT_") ~
      ("ncicde" -> "https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#") ~
      ("example" -> "http://example.org/csv2cadsr#")

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
                  case JObject(obj) =>
                    EnumMapping(
                      colName,
                      prop,
                      extractJString(obj, "value").getOrElse(""),
                      extractJString(obj, "description"),
                      extractJString(obj, "caDSRValue"),
                      extractJString(obj, "conceptURI")
                    )
                  case unk => throw new RuntimeException(s"Expected object as enumValue, but found $unk")
                })
                allEnumMappings.appendAll(enumMappings)
                val conceptsWithIDs = enumMappings.filter(_.conceptURI.nonEmpty)
                val conceptsWithoutIDs = enumMappings.filter(_.conceptURI.isEmpty)
                if (conceptsWithIDs.nonEmpty && conceptsWithoutIDs.nonEmpty) {
                  s"""
                     |    sh:nodeKind sh:IRIOrLiteral ;
                     |    sh:in (
                     |      ${conceptsWithIDs.flatMap(_.conceptURI).map(uri => s"<$uri>").mkString("\n      ")}
                     |      ${conceptsWithoutIDs.map(mapping => mapping.caDSRValue.getOrElse(mapping.value)).map(_.prepended('"').appended('"') + "^^<xsd:string>").mkString("\n      ")}
                     |    )
                     |""".stripMargin
                } else if (conceptsWithIDs.nonEmpty && conceptsWithoutIDs.isEmpty) {
                  s"""
                     |    sh:nodeKind sh:IRI ;
                     |    sh:in (
                     |      ${conceptsWithIDs.flatMap(_.conceptURI).map(uri => s"<$uri>").mkString("\n      ")}
                     |    )
                     |""".stripMargin
                } else if (conceptsWithIDs.isEmpty && conceptsWithoutIDs.nonEmpty) {
                  s"""
                     |    sh:nodeKind sh:Literal ;
                     |    xsd:dataType xsd:string ;
                     |    sh:in (
                     |      ${conceptsWithoutIDs.map(mapping => mapping.caDSRValue.getOrElse(mapping.value)).map(_.prepended('"').appended('"') + "^^<xsd:string>").mkString("\n      ")}
                     |    )
                     |""".stripMargin
                } else {
                  // No idea what this type is.
                  ""
                }

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

            s"""
               |  sh:property [
               |    sh:name "$colName" ;
               |    sh:description "${extractJString(prop, "description").getOrElse("")}" ;
               |    sh:path <${getURIForColumn(colName)}> ;
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
          s"""
            |<${enumMapping.conceptURI.get}>
            |  rdfs:label "${enumMapping.caDSRValue.getOrElse("")}" ;
            |  dc:description "${enumMapping.description.getOrElse("")}" ;
            |  example:fromCDE <${getCDEURI(enumMapping.colObject)}>
            |.
            |""".stripMargin
        }

      // Write it all out.
      val shaclWriter = new BufferedWriter(new FileWriter(shaclFilename))
      shaclWriter.write(
        s"""
           |@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
           |@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
           |@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
           |@prefix dc: <http://purl.org/dc/elements/1.1/> .
           |@prefix sh: <http://www.w3.org/ns/shacl#> .
           |@prefix example: <http://example.org/csv2cadsr#> .
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

    if (generateJSONSchema) {
      // We generate outputPrefix + ".schema.json"
    }

    // Step 2. Create a JSON-LD Instance for each row in the input data.
    dataWithHeaders.zipWithIndex
      .foreach({
        case (row, index) =>
          val values: Seq[JField] = headerRow
            .map(colName => (colName, row.get(colName)))
            .map({
              case (colName, Some(value)) => {
                val prop = properties.get(colName).getOrElse(JObject(List()))

                val mappings: Seq[JObject] = (prop \ "enumValues") match {
                  case JArray(enumValues) => {
                    enumValues flatMap { enumValue =>
                      val mappingValue = (enumValue \ "value") match {
                        case JString(v) => v
                      }
                      // scribe.info(s"Comparing '$value' with '$mappingValue'")
                      if (value == mappingValue) {
                        // Got a map!
                        val uri = (enumValue \ "conceptURI") match {
                          case JString(v) => v
                          case JNothing   => ""
                        }
                        val caDSRValue = (enumValue \ "caDSRValue") match {
                          case JString(v) => v
                          case JNothing   => ""
                        }
                        if (uri.isEmpty) Some(
                          // We get here if one of the values was mapped to a caDSR value which doesn't have a concept URI.
                          // TODO: we might need to replace this code in ToCEDAR.
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

          val instance =
            ("@id" -> s"${inputFile.toURI}#row$index") ~
            ("@type" -> "example:ExportSchema") ~
            ("@context" -> context) ~
            JObject(values.toList)

          val instanceFilename = new File(outputPrefix + s".instance.$index.jsonld")
          val instanceWriter = new FileWriter(instanceFilename)
          instanceWriter.append(pretty(render(instance)))
          instanceWriter.close()
          scribe.info(s"Wrote out row $index to $instanceFilename")

        case unk => throw new RuntimeException(s"Unexpected value in JSON mappings file: ${unk}")
      })
  }
}
