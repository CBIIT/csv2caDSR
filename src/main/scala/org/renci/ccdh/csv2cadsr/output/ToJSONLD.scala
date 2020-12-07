package org.renci.ccdh.csv2cadsr.output

import java.io.{File, FileWriter}

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

  /** Convert a column name into a URI for use as an identifier.
    */
  def convertColNameToURI(colName: String, props: JObject): String = {
    (props \ "@id") match {
      // Do we have an '@id'?
      case JString(id) => id
      case _ => {
        (props \ "caDSR") match {
          case JString(cdeId) if cdeId.nonEmpty => {
            // Build an URI using the cdeId
            val cdeVersion = (props \ "caDSRVersion") match {
              case JString(cdeVersion) if cdeVersion.nonEmpty => cdeVersion
              case _ => "1.0" // Default to 1.0
            }

            s"https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#/search?publicId=${cdeId}&version=${cdeVersion}"
          }
          case _ => {
            // If we can't figure this out, let's build a dummy URI from the colName.
            // As per https://stackoverflow.com/a/40415059/27310
            "http://example.org/csv2caDSR#" + colName.replaceAll("[^A-Za-z0-9\\-._~()'!*:@,;]", "_")
          }
        }
      }
    }
  }

  def writeJSONLD(
    reader: CSVReader,
    properties: Map[String, JValue],
    outputPrefix: String,
    includeJSONSchema: Boolean
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
      ("ncit" -> "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#") ~
      ("ncicde" -> "https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#")

    val propertyTypesMap = mutable.Map[String, String]()
    val contextProperties = JObject(headerRow map { colName =>
      val colURI = properties.get(colName) match {
        case Some(prop: JObject) => {
          (prop \ "@type").toOption match {
            case Some(JString(typeName)) => propertyTypesMap.put(colName, typeName)
            case _ =>
          }

          convertColNameToURI(colName, prop)
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

    if (includeJSONSchema) {
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
                        if (uri.isEmpty) None
                        else
                          Some(
                            ("@id" -> uri) ~
                              ("rdfs:label" -> caDSRValue)
                            // TODO: include verbatim values here?
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
