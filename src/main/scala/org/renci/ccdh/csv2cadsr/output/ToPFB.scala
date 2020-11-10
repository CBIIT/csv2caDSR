package org.renci.ccdh.csv2cadsr.output

import java.io.File
import java.util.Collections

import com.github.tototoshi.csv.CSVReader
import org.apache.avro._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.json4s.{JArray, JString}
import org.json4s.JsonAST.{JObject, JValue}
import org.renci.ccdh.csv2cadsr.output.pfb.PFBSchemas

import scala.collection.mutable
import scala.jdk.javaapi.CollectionConverters

/**
  * Converts a CSV file to a PFB file with annotations.
  */
object ToPFB {
  def writePFB(reader: CSVReader, properties: Map[String, JValue], pfbFilename: File): Unit = {
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // Step 1. Create a schema for the output.
    val schemaBuilder = SchemaBuilder
      .record("export") // TODO: name this after the input filename, I guess?
    var fieldsBuilder = schemaBuilder.fields()

    // TODO: this should be colName, not rowName.
    val fieldInfo = mutable.Map[String, JObject]()
    val fieldTypes = mutable.Map[String, String]()
    headerRow foreach { rowName =>
      properties.get(rowName) foreach { case property: JObject =>
        val fieldType: String = property.values.get("type").flatMap({
          case str: JString => Some(str.s)
          case _ => None
        }) match {
          case Some("integer") => "long"  // We could also use "int" here if we want 32 bit ints.
          case Some("number") => "string" // We'll store it as a string and let it be reparsed (hopefully into BigDecimal) at the other end.
          case Some(str) => str
          case None => "string"
        }
        fieldTypes.put(rowName, fieldType)
        fieldInfo.put(rowName, property)

        if (property.values.contains("enum")) {
          val enumValuesOld: Seq[String] = (property \ "enum").toOption match {
            // Enumerated values in Avro must match the regex /^[A-Za-z_][A-Za-z0-9_]*/ -- just like Avro names.
            case Some(JString(str)) if str.matches("/^[A-Za-z_][A-Za-z0-9_]*/") => Seq(str)
            case Some(arr: JArray) => arr.arr.flatMap({
              case JString(str) if str.matches("/^[A-Za-z_][A-Za-z0-9_]*/") => Some(str)
              case _ => None
            })
            // If this isn't a string or an array of strings, we can't convert it to enumValues.
            case _ => Seq()
          }
          val enumValues = Seq()
          if (enumValues.isEmpty) {
            // We have failed to build an enum -- let's just fallback to using a string.
            fieldsBuilder = fieldsBuilder
              .name(rowName.replaceAll("\\W","_"))
              .`type`(Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING)
              ))
              .noDefault()
          } else {
            fieldsBuilder = fieldsBuilder
              .name(rowName.replaceAll("\\W", "_"))
              .`type`(Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.createEnum(
                  rowName.replaceAll("\\W", "_") + "_t",
                  s"Enumeration of field '${rowName}'",
                  "",
                  CollectionConverters.asJava(enumValues)
                )
              ))
              .noDefault()
          }
        } else {
          fieldsBuilder = fieldsBuilder
            .name(rowName.replaceAll("\\W","_"))
            .`type`(Schema.createUnion(
              Schema.create(Schema.Type.NULL),
              Schema.create(Schema.Type.valueOf(fieldType.toUpperCase))
            ))
            .noDefault()
        }
      case value: JValue => new RuntimeException(s"Expected JObject but obtained $value")
      }
    }

    val exportSchema = fieldsBuilder.endRecord()
    val schema = pfb.PFBSchemas.generatePFBForSchemas(Seq(exportSchema))
    scribe.info(s"Created schema: ${schema.toString(true) }")

    val writer = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(schema, pfbFilename)
    // TODO: It'd be nice to write this out to outputWriter like all the other CSVToOutputs,
    // but putting an OutputStreamWriter() around that messes up the output for some reason.

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
