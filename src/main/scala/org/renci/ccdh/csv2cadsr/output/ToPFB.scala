package org.renci.ccdh.csv2cadsr.output

import java.io.File
import java.util.Collections

import com.github.tototoshi.csv.CSVReader
import org.apache.avro._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.{JArray, JString}
import org.renci.ccdh.csv2cadsr.output.pfb.PFBSchemas

import scala.collection.mutable
import scala.jdk.javaapi.CollectionConverters

/**
  * Converts a CSV file to a PFB file with annotations.
  */
object ToPFB {
  /**
    * Field names (and therefore enum values as well) in Avro is made of name components ([A-Za-z_][A-Za-z0-9_]*)
    * separated by '.'s (see https://avro.apache.org/docs/1.8.1/spec.html#names). The Avro format uses a workaround
    * for this (https://github.com/uc-cdis/pypfb/blob/0eed3b4b19eb7b6bdcf3f57334f840e2ad8388cd/doc/README.md#enum):
    * characters can be encoded as `_1234_`, which is translated to \u1234 (i.e. U+1234) in the Basic Multilingual Plane.
    * As a optimization, `_12_` is translated to U+0012 and `_123_` to U+0123.
    *
    * Avro names can't start with a digit, so for simplicity's sake, we'll translate digits into Unicode as well.
    * Since paired `_`s will be misinterpreted, we must translate all characters outside of [A-Za-z\.] into this format.
    *
    * TODO: Confirm that this handles multi-code point characters correctly.
    * TODO: Memoize this.
    *
    * @param fieldName The field name or enum value to be translated.
    * @return An encoded format that is strictly only contains characters [A-Za-z\.]*
    */
  def mapFieldNameToPFB(fieldName: String): String = fieldName.map({
    case '.' => "."
    case ch if Character.isLetter(ch) => ch.toString
    case ch => s"_${Integer.toHexString(ch)}_"
  }).mkString

  def writePFB(reader: CSVReader, properties: Map[String, JValue], pfbFilename: File): Unit = {
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // Step 1. Create a schema for the output.
    val schemaBuilder = SchemaBuilder
      .record("export") // TODO: name this after the output filename, I guess?
    var fieldsBuilder = schemaBuilder.fields()

    // Go through the properties, identify permissible values and add them to the schema.
    val fieldTypes = mutable.Map[String, String]()
    val harmonizationMappings = mutable.Map[String, JValue]()
    headerRow foreach { colName =>
      val props: Option[JValue] = properties.get(colName)
      props foreach { case property: JObject =>
        // Figure out the field type for this column.
        val fieldType: String = property.values.get("type").flatMap({
          case str: JString => Some(str.s)
          case _ => None
        }) match {
          case Some("integer") => "long"  // We could also use "int" here if we want 32 bit ints.
          case Some("number") => "string" // We'll store it as a string and let it be reparsed (hopefully into BigDecimal) at the other end.
          case Some(str) => str
          case None => "string"
        }
        fieldTypes.put(colName, fieldType)

        // Look for permissible values on this property.
        if (property.values.contains("permissibleValues")) {
          // scribe.info(s"property \\ permissibleValues: ${(property \ "permissibleValues").toOption}")
          val enumValues = (property \ "enumValues")
          val permissibleValues: Seq[String] = enumValues match {
            case JArray(arr) => arr.map({
              case obj: JObject => obj.values.getOrElse("caDSRValue", "").asInstanceOf[String]
              case unk => throw new RuntimeException(s"Cannot read value in array in enumValues in property '$colName': $unk")
            }).filter(_.nonEmpty).map(mapFieldNameToPFB)
            case unk => throw new RuntimeException(s"Cannot read value in enumValues in property '$colName': $unk")
          }
          scribe.debug(s"enumValues '$enumValues' gives us permissibleValues: $permissibleValues")

          if (permissibleValues.isEmpty) {
            // We have failed to build an enum -- let's just fallback to using a string.
            fieldsBuilder = fieldsBuilder
              .name(mapFieldNameToPFB(colName))
              .`type`(Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING)
              ))
              .noDefault()
          } else {
            // We have permissible values we can work with!
            harmonizationMappings.put(colName, property \ "enumValues")

            fieldsBuilder = fieldsBuilder
              .name(mapFieldNameToPFB(colName))
              .`type`(Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.createEnum(
                  mapFieldNameToPFB(colName) + "_t",
                  s"Enumeration of field '${colName}'",
                  "",
                  CollectionConverters.asJava(permissibleValues)
                )
              ))
              .noDefault()
          }
        } else {
          // If there are no permissible values, we just need to return a nullable field of the specified type.
          fieldsBuilder = fieldsBuilder
            .name(mapFieldNameToPFB(colName))
            .`type`(Schema.createUnion(
              Schema.create(Schema.Type.NULL),
              Schema.create(Schema.Type.valueOf(fieldType.toUpperCase))
            ))
            .noDefault()
        }
      case value => new RuntimeException(s"Expected JObject but obtained $value for property $colName")
      }
    }

    // Build an overall "export schema" by adding our schema to that needed by PFB.
    val exportSchema = fieldsBuilder.endRecord()
    val schema = pfb.PFBSchemas.generatePFBForSchemas(Seq(exportSchema))
    // scribe.info(s"Created schema: ${schema.toString(true) }")

    // Step 2. Create the metadata object. This records information on how to interpret each column in each data file.
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(schema, pfbFilename)  // I ran into some problems when writing this to an existing BufferedWriter,
                                                // so just creating a new file seems to be the way to go here.

    // We need to create a Metadata entry for describing these fields.
    val metadataNode = new GenericData.Record(PFBSchemas.nodeSchema)
    metadataNode.put("name", "export")
    metadataNode.put("ontology_reference", "")
    metadataNode.put("values", Collections.EMPTY_MAP)
    metadataNode.put("links", Collections.EMPTY_LIST)
    metadataNode.put("properties", CollectionConverters.asJava(headerRow.flatMap({ rowName =>
      properties.get(rowName) map {
        case property: JObject =>
          val cdeIdOpt = property.values.get("caDSR")
          val cdeIdVersion = property.values.get("caDSRVersion")
          val description = property.values.get("description")

          val prop = new GenericData.Record(PFBSchemas.propertySchema)
          prop.put("name", rowName)
          prop.put("ontology_reference", description.getOrElse(""))

          // Record caDSR information when present.
          cdeIdOpt match {
            case None | Some("") => // Nothing we can do.
              prop.put("values", Collections.EMPTY_MAP)
            case Some(cdeId) =>
              prop.put("values", CollectionConverters.asJava(Map(
                  "source" -> "caDSR",
                  "cde_id" -> cdeId,
                  "cde_version" -> cdeIdVersion.getOrElse(""),
                  "term_url" -> s"https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#/search?publicId=${cdeId}&version=${cdeIdVersion.getOrElse("1.0")}"
                )
              ))
          }

          prop
        case value: JValue => throw new RuntimeException(s"Unable to interpret property '$value': not a JObject")
      }
    })))

    val objectMeta = new GenericData.Record(PFBSchemas.metadataSchema)
    objectMeta.put("nodes", java.util.List.of(metadataNode))
    objectMeta.put("misc", Collections.EMPTY_MAP)

    val metadata = new GenericData.Record(schema)
    metadata.put("id", null)
    metadata.put("name", "Metadata")
    metadata.put("object", objectMeta)
    metadata.put("relations", Collections.EMPTY_LIST)

    dataFileWriter.append(metadata)

    // Step 3. Write data into Avro file.
    dataWithHeaders.zipWithIndex.foreach({ case (row, index) =>
      val export = new GenericData.Record(exportSchema)
      row.keys.map({ colName =>
        val value = row.getOrElse(colName, "")
        val fieldType = fieldTypes.getOrElse(colName, "string")

        // If this is an enum field, we need to translate from the verbatim to the harmonized value.
        val mappedValue: Any = if (value == "") null else if(!harmonizationMappings.contains(colName)) {
          fieldType match {
            case "long" => value.toLong
            case _ => value
          }
        } else {
          val mappedValues: Seq[String] = harmonizationMappings.get(colName) match {
            case Some(JArray(list)) => list flatMap {
                case JObject(mappingValues) => {
                  // Does this mapping apply to this particular value?
                  // (Note that if multiple mappings are provided for the same value, we throw an exception later in this process.
                  val mapping = mappingValues.toMap
                  val inputValue = mapping.get("value")
                  if (inputValue.getOrElse("").equals(JString(value))) {
                    mapping.get("caDSRValue") match {
                      case Some(JString("")) => {
                        throw new RuntimeException(s"Could not map verbatim value '$inputValue' in $colName: no mapping in input JSON file")
                      }
                      case Some(JString(str)) => {
                        scribe.info(s"Mapping verbatim value '$inputValue' in $colName to: $str")
                        Some(mapFieldNameToPFB(str))
                      }
                      case unk => throw new RuntimeException(s"Unexpected value in caDSRValue field of harmonization mappings of ${colName}: ${unk}")
                    }
                  } else {
                    // Irrelevant mapping, ignore.
                    None
                  }
                }
                case unk => throw new RuntimeException(s"Unexpected value in harmonization mappings of ${colName}: ${unk}")
              }
            case unk => throw new RuntimeException(s"Unexpected value in harmonization mappings of ${colName}: ${unk}")
          }

          if (mappedValues.isEmpty) null
          else if(mappedValues.length > 1) throw new RuntimeException(s"Too many mapped values obtained for ${colName}: ${mappedValues.mkString("|")}")
          else {
            // We only have one mapped value! Return as an Enum symbol.
            // scribe.info(s"Looking up information on colName $colName (= ${mapFieldNameToPFB(colName)})")
            val fieldTypes = exportSchema.getField(mapFieldNameToPFB(colName)).schema().getTypes
            val enumTypes = CollectionConverters.asScala(fieldTypes).filter(_.getName != "null")

            scribe.info(s"Enum values for ${colName}: ${enumTypes.map(_.getEnumSymbols)}")

            if (enumTypes.length != 1) {
              throw new RuntimeException(s"Found multiple enumTypes, expected only one: ${enumTypes}")
            } else {
              GenericData.get().createEnum(mappedValues.head, enumTypes.head)
            }
          }
        }

        scribe.debug(s"Output(${mapFieldNameToPFB(colName)}, ${if (mappedValue == null) "<null>" else mappedValue})")

        export.put(
          mapFieldNameToPFB(colName),
          mappedValue
        )
      })

      // Write this datum to the Avro file.
      val datum = new GenericData.Record(schema)
      datum.put("id", "row" + index)
      datum.put("name", "export")
      datum.put("object", export)
      datum.put("relations", Collections.EMPTY_LIST)

      dataFileWriter.append(datum)
    })

    dataFileWriter.close()
  }
}
