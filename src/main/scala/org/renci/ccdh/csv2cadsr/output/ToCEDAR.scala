package org.renci.ccdh.csv2cadsr.output

import java.io.{File, FileReader, FileWriter}
import java.time.format.DateTimeFormatter
import java.util.Calendar

import com.github.tototoshi.csv.CSVReader
import org.json4s
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, JArray, JField, JNothing, JObject, JString}

/**
  * Converts a harmonized CSV file to CEDAR instance data.
  *
  * Since CEDAR manages CEDAR templates and CEDAR instance data separately, we will generate two files:
  *   - ${cedarFilename}.template.json defines a CEDAR template for the harmonized data. This will include schema
  *     information for the fields as well as information on the CDEs that are being used.
  *   - ${cedarFilename}.instance[N].json provides information on *one* CEDAR instance. It includes a link to the
  *     template (by URI) as well as the data in JSON.
  *
  * TODO: To be able to quickly evaluate these formats, we'll start with some hard-coded strings. But eventually,
  *   we'll want to make the base URIs configurable.
  */
class ToCEDAR(
  baseURI: String = "http://ggvaidya.com/csv2caDSR/export#",
  pavCreatedBy: String = "https://metadatacenter.org/users/ebca7bcb-4e1a-495b-919e-31884aa89461" // Gaurav's user ID on CEDAR.
) {
  // We use default formats to read values from JSON objects.
  implicit val formats = DefaultFormats

  /** This helper class is used to wrap caDSR mappings. */
  case class CEDARClassConstraint(
    uri: String,
    cadsrLabel: String,
    _type: String,
    datasetLabel: String,
    source: String
  ) {
    def toJSON =
      ("uri" -> uri) ~
        ("prefLabel" -> cadsrLabel) ~
        ("type" -> _type) ~
        ("label" -> cadsrLabel) ~
        ("source" -> source)
  }

  /**
    * Write information to CEDAR files (and optionally upload them to the CEDAR Workbench).
    *
    * @param inputCSVFile The CSV file used as input.
    * @param reader A CSVReader used to read this file.
    * @param properties A Map of properties from the JSON mapping file.
    * @param requiredPropNames A Set of required properties by name.
    * @param cedarBasename The basename used for CEDAR files. We store the template in `${cedarBasename}.template.json`
    *                      and instance data in `${cedarBasename}.instance.${index}.json`, where $index starts from zero.
    * @param uploadToCedar Whether or not to upload the template and instance data to the CEDAR workbench.
    * @param cedarUploadFolderURL Optionally, the CEDAR folder URL to upload template and instance data to.
    *                             (e.g. https://repo.metadatacenter.org/folders/3892a1ba-e253-4918-96ec-040b649164cf)
    */
  def writeCEDAR(
    inputCSVFile: File,
    reader: CSVReader,
    properties: Map[String, JValue],
    requiredPropNames: Set[String],
    cedarBasename: File,
    uploadToCedar: Boolean = false,
    cedarUploadFolderURL: Option[String] = None
  ): Unit = {
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // Record the created-on time for these records.
    val pavCreatedOn =
      DateTimeFormatter.ISO_INSTANT.format(Calendar.getInstance().getTime.toInstant)

    // Load CEDAR apiKey for POST requests. This should be stored in a file in ~/.cedar.properties,
    // which should contain a string: "apiKey=9999999999999999999999999999999999999999999999999999999999999999"
    val propertiesFile = new File(System.getProperty("user.home"), ".cedar.properties")
    val utilProperties = new java.util.Properties()
    utilProperties.load(new FileReader(propertiesFile))
    val apiKey = utilProperties.getOrDefault("apiKey", "").asInstanceOf[String]
    if (apiKey.isEmpty)
      throw new RuntimeException(s"No apiKey present in config file $propertiesFile.")

    // Step 1. Create a CEDAR Template for the harmonization information.
    // TODO none of this quite makes sense as JSON Schema, which I suspect is a bug in the way CEDAR generates these files, but I might be wrong.
    // Once we have a working file, we should simplify this file as much as possible to see what is minimally required.

    // We start by creating the CEDAR template properties.
    // The following properties appear to be defined identically in all CEDAR templates.
    val cedarTemplateBaseProperties =
      ("schema:isBasedOn" ->
        ("type" -> "string") ~
          ("format" -> "uri")) ~
        ("schema:name" ->
          ("type" -> "string") ~
            ("minLength" -> 1)) ~
        ("pav:createdBy" ->
          ("type" -> List("string", "null")) ~
            ("format" -> "uri")) ~
        ("pav:createdOn" -> (
          ("type" -> List("string", "null")) ~
            ("format" -> "date-time")
        )) ~
        ("pav:lastUpdatedOn" -> (
          ("type" -> List("string", "null")) ~
            ("format" -> "date-time")
        )) ~
        ("oslc:modifiedBy" -> (
          ("type" -> List("string", "null")) ~
            ("format" -> "uri")
        )) ~
        ("pav:derivedFrom" -> (
          ("type" -> "string") ~
            ("format" -> "uri")
        )) ~
        ("@id" -> (
          ("type" -> List("string", "null")) ~
            ("format" -> "uri")
        )) ~
        ("schema:description" -> ("type" -> "string")) ~
        ("@type" ->
          ("oneOf" -> JArray(
            List(
              ("type" -> "string") ~
                ("format" -> "uri"),
              ("minItems" -> 1) ~
                ("type" -> "array") ~
                ("uniqueItems" -> true) ~
                ("items" -> (
                  ("type" -> "string") ~
                    ("format" -> "uri")
                ))
            )
          ))) ~
        ("@context" ->
          ("additionalProperties" -> false) ~
            ("type" -> "object") ~
            ("required" -> List(
              "xsd",
              "pav",
              "schema",
              "oslc",
              "schema:isBasedOn",
              "schema:name",
              "schema:description",
              "pav:createdOn",
              "pav:createdBy",
              "pav:lastUpdatedOn",
              "oslc:modifiedBy"
            )) ~
            ("properties" ->
              ("schema:isBasedOn" -> (
                ("type" -> "object") ~
                  ("properties" -> (
                    "@type" -> (
                      ("enum" -> List("@id")) ~
                        ("type" -> "string")
                    )
                  ))
              )) ~
                ("schema:name" -> (
                  ("type" -> "object") ~
                    ("properties" -> (
                      "@type" -> (
                        ("enum" -> List("xsd:string")) ~
                          ("type" -> "string")
                      )
                    ))
                )) ~
                ("schema" ->
                  ("enum" -> List("http://schema.org/")) ~
                    ("type" -> "string") ~
                    ("format" -> "uri")) ~
                ("pav" ->
                  ("enum" -> List("http://purl.org/pav/")) ~
                    ("type" -> "string") ~
                    ("format" -> "uri")) ~
                ("xsd" ->
                  ("enum" -> List("http://www.w3.org/2001/XMLSchema#")) ~
                    ("type" -> "string") ~
                    ("format" -> "uri")) ~
                ("oslc" ->
                  ("enum" -> List("http://open-services.net/ns/core#")) ~
                    ("type" -> "string") ~
                    ("format" -> "uri")) ~
                ("rdfs" ->
                  ("enum" -> List("http://www.w3.org/2000/01/rdf-schema#")) ~
                    ("type" -> "string") ~
                    ("format" -> "uri")) ~
                ("oslc:modifiedBy" ->
                  ("type" -> "object") ~
                    ("properties" ->
                      ("@type" -> (
                        ("enum" -> List("@id")) ~
                          ("type" -> "string")
                      )))) ~
                ("rdfs:label" ->
                  ("type" -> "object") ~
                    ("properties" ->
                      ("@type" ->
                        ("enum" -> List("xsd:string")) ~
                          ("type" -> "string")))) ~
                ("pav:derivedFrom" ->
                  ("type" -> "object") ~
                    ("properties" -> (
                      ("@type" -> (
                        ("enum" -> List("@id")) ~
                          ("type" -> "string")
                      ))
                    ))) ~
                ("skos" ->
                  ("enum" -> List("http://www.w3.org/2004/02/skos/core#")) ~
                    ("type" -> "string") ~
                    ("format" -> "uri")) ~
                ("schema:description" -> (
                  ("type" -> "object") ~
                    ("properties" -> (
                      ("@type" ->
                        ("enum" -> List("xsd:string")) ~
                          ("type" -> "string"))
                    ))
                )) ~
                ("pav:lastUpdatedOn" ->
                  ("type" -> "object") ~
                    ("properties" ->
                      ("@type" -> (
                        ("enum" -> List("xsd:dateTime")) ~
                          ("type" -> "string")
                      )))) ~
                ("pav:createdOn" ->
                  ("type" -> "object") ~
                    ("properties" -> ("@type" -> (
                      ("enum" -> List("xsd:dateTime")) ~
                        ("type" -> "string")
                    )))) ~
                ("skos:notation" -> (
                  ("type" -> "object") ~
                    ("properties" -> (
                      ("@type" ->
                        ("enum" -> List("xsd:string")) ~
                          ("type" -> "string"))
                    ))
                )) ~
                ("id" ->
                  ("enum" -> List("http://purl.org/dc/terms/identifier"))) ~
                ("pav:createdBy" -> (
                  ("type" -> "object") ~
                    ("properties" -> (
                      ("@type" -> (
                        ("enum" -> List("@id")) ~
                          ("type" -> "string")
                      ))
                    ))
                ))))

    // We accumulate property descriptions in this object.
    var propertyDescriptions = JObject()

    // We generate template properties for each column by looking up its type, enumeration and other information.
    val cedarTemplatePropertiesForCols = headerRow flatMap { colName =>
      {
        properties.get(colName) match {
          case Some(prop: JObject) => {
            // Convert property type.
            val numberType = (prop \ "type") match {
              case JString("integer") =>
                Some("xsd:long") // We can also use xsd:int if we want short ints.
              case JString("number") => None
              case _                 => None
            }

            // Is a value required for this column name?
            val requiredValue = requiredPropNames.contains(colName)

            // What value constraints should be applied to this property? We only suppose classes here,
            // but CEDAR includes many other kinds of constraints
            // (see https://more.metadatacenter.org/tools-training/outreach/cedar-template-model).
            val valueConstraints = if (prop.values.contains("enumValues")) {
              val enumValues: Seq[CEDARClassConstraint] = (prop \ "enumValues") match {
                case JArray(arr) =>
                  arr
                    .map({
                      case obj: JObject =>
                        CEDARClassConstraint(
                          (obj \ "conceptURI").extract[String],
                          (obj \ "caDSRValue").extract[String],
                          "OntologyClass",
                          (obj \ "value").extract[String],
                          "NCIT"
                        )
                      case unk =>
                        throw new RuntimeException(
                          s"Cannot read value in array in enumValues in property '$colName': $unk"
                        )
                    })
                    .filter(
                      ccc => ccc.cadsrLabel != "" && ccc.datasetLabel != ""
                    ) // Remove unlabeled entries.
                case unk =>
                  throw new RuntimeException(
                    s"Cannot read value in enumValues in property '$colName': $unk"
                  )
              }

              ("_ui" ->
                ("inputType" -> "textfield") // I wonder if we can change this to radio button or checkbox?
              ) ~
                ("_valueConstraints" ->
                  ("requiredValue" -> requiredValue) ~
                    ("ontologies" -> JArray(List())) ~
                    ("valueSets" -> JArray(List())) ~
                    ("classes" -> JArray(enumValues.map(_.toJSON).toList)) ~
                    ("branches" -> JArray(List())) ~
                    ("multipleChoice" -> false))
              // ~ ("required" -> JArray(List("@id", "rdfs:label")))
              // TODO: I would expect this to work here, but it causes an error on upload to CEDAR.
            } else if (numberType.nonEmpty)
              ("_ui" ->
                ("inputType" -> "numeric")) ~
                ("_valueConstraints" ->
                  ("requiredValue" -> requiredValue) ~
                    ("numberType" -> numberType.get)) ~
                ("required" -> JArray(List("@value", "@type")))
            else
              ("_ui" ->
                ("inputType" -> "textfield")) ~
                ("_valueConstraints" ->
                  ("requiredValue" -> requiredValue)) ~
                ("required" -> JArray(List("@value")))

            // Convert property from our format to that used by CEDAR templates.
            val property = {
              ("$schema" -> "http://json-schema.org/draft-04/schema#") ~
                ("@context" ->
                  ("schema" -> "http://schema.org/") ~
                    ("pav" -> "http://purl.org/pav/") ~
                    ("xsd" -> "http://www.w3.org/2001/XMLSchema#") ~
                    ("bibo" -> "http://purl.org/ontology/bibo/") ~
                    ("skos" -> "http://www.w3.org/2004/02/skos/core#") ~
                    ("oslc" -> "http://open-services.net/ns/core#") ~

                    ("schema:name" -> ("@type" -> "xsd:string")) ~
                    ("pav:createdOn" -> ("@type" -> "xsd:dateTime")) ~
                    ("oslc:modifiedBy" -> ("@type" -> "@id")) ~
                    ("pav:lastUpdatedOn" -> ("@type" -> "xsd:dateTime")) ~
                    ("skos:prefLabel" -> ("@type" -> "xsd:string")) ~
                    ("skos:altLabel" -> ("@type" -> "xsd:string")) ~
                    ("schema:description" -> ("@type" -> "xsd:string")) ~
                    ("pav:createdBy" -> ("@type" -> "@id"))) ~
                ("@type" -> "https://schema.metadatacenter.org/core/TemplateField") ~
                ("schema:schemaVersion" -> "1.6.0") ~
                ("additionalProperties" -> false) ~
                ("type" -> "object") ~
                ("properties" -> (
                  ("@value" ->
                    ("type" -> List("string", "null"))) ~
                    ("rdfs:label" ->
                      ("type" -> List("string", "null"))) ~
                    ("@type" ->
                      ("type" -> "string") ~
                        ("format" -> "uri")) ~
                    ("@id" ->
                      ("type" -> "string") ~
                        ("format" -> "uri"))
                )) ~
                // Field information.
                ("@id" -> (baseURI + colName.replaceAll("\\W", "_"))) ~
                ("schema:name" -> colName) ~
                ("schema:description" -> prop \ "description") ~
                ("skos:prefLabel" -> colName) ~
                ("title" -> s"$colName field schema") ~
                ("description" -> s"$colName field schema generated by csv2caDSR") ~
                // Value information.
                valueConstraints ~
                // Field metadata.
                ("pav:createdBy" -> pavCreatedBy) ~
                ("pav:createdOn" -> pavCreatedOn) ~
                ("oslc:modifiedBy" -> pavCreatedBy) ~
                ("pav:lastUpdatedOn" -> pavCreatedOn)
            }

            // Add this property description to the list of property descriptions we've generated previously.
            propertyDescriptions = propertyDescriptions ~ (colName -> (prop \ "description"))

            Some(JField(colName, property))
          }
          case Some(unk) =>
            throw new RuntimeException(s"Unknown object in property information: $unk.")
          case None =>
            throw new RuntimeException(s"Property $colName not present in JSON mapping file.")
          case unk => throw new RuntimeException(s"Property $colName has unexpected object: $unk.")
        }
      }
    }

    // Assemble CEDAR template properties by combining the base properties (that are the same for all templates) and
    // the properties we have generated for the columns in this dataset.
    val cedarTemplateProperties = cedarTemplateBaseProperties ~ cedarTemplatePropertiesForCols

    // Create a CEDAR template, including the template properties generated above.
    // The base template appears to be the same for all CEDAR templates.
    val baseCEDARTemplate: JObject =
      ("@context" ->
        // Prefixes.
        ("oslc" -> "http://open-services.net/ns/core#") ~
          ("schema" -> "http://schema.org/") ~
          ("xsd" -> "http://www.w3.org/2001/XMLSchema#") ~
          ("bibo" -> "http://purl.org/ontology/bibo/") ~
          ("pav" -> "http://purl.org/pav/") ~

          // Standard properties.
          ("pav:lastUpdatedOn" -> ("@type" -> "xsd:dateTime")) ~
          ("pav:createdBy" -> ("@type" -> "@id")) ~
          ("schema:name" -> ("@type" -> "xsd:string")) ~
          ("pav:createdOn" -> ("@type" -> "xsd:dateTime")) ~
          ("oslc:modifiedBy" -> ("@type" -> "@id")) ~
          ("schema:description" -> ("@type" -> "xsd:string"))) ~
        ("type" -> "object") ~
        ("additionalProperties" -> false)

    // Generate a full CEDAR template by combining:
    //  1. The base CEDAR template, which is common for all templates,
    //  2. Template-level metadata, such as title, description and creation time,
    //  3. _ui information, such as the order of fields and their labels and descriptions, and
    //  4. The properties that we generated from column descriptions above.
    val cedarTemplate: JObject = baseCEDARTemplate ~
      ("$schema" -> "http://json-schema.org/draft-04/schema#") ~
      ("@type" -> "https://schema.metadatacenter.org/core/Template") ~
      ("title" -> s"csv2caDSR CEDAR Template Export ($pavCreatedOn)") ~ // This appears to be ignored.
      ("description" -> s"csv2caDSR CEDAR Template Export from ${inputCSVFile}") ~
      ("schema:name" -> s"csv2caDSR CEDAR Template Export ($pavCreatedOn)") ~
      ("schema:description" -> s"CEDAR Template Export based on harmonized data from ${inputCSVFile}") ~
      ("pav:createdOn" -> pavCreatedOn) ~
      ("pav:createdBy" -> pavCreatedBy) ~
      ("pav:lastUpdatedOn" -> pavCreatedOn) ~
      ("bibo:status" -> "bibo:draft") ~
      ("pav:version" -> "0.0.1") ~
      ("schema:schemaVersion" -> "1.6.0") ~
      ("required" -> ((Seq(
        "@context",
        "@id",
        "schema:isBasedOn",
        "schema:name",
        "schema:description",
        "pav:createdOn",
        "pav:createdBy",
        "pav:lastUpdatedOn",
        "oslc:modifiedBy"
      ) ++ headerRow) map (JString(_)))) ~
      ("_ui" -> (
        ("order" -> headerRow) ~
          ("pages" -> JArray(List())) ~
          ("propertyLabels" -> JObject(headerRow.map(rowName => (rowName, JString(rowName))))) ~
          ("propertyDescriptions" -> propertyDescriptions)
      )) ~
      ("properties" -> cedarTemplateProperties)

    // Write out this template to "$cedarBasename.template.json".
    val templateFilename = new File(cedarBasename.getAbsolutePath + ".template.json")
    val templateWriter = new FileWriter(templateFilename)
    templateWriter.append(pretty(render(cedarTemplate)))
    templateWriter.close()

    // Optionally, POST this template to CEDAR.
    // Uses API as https://resource.metadatacenter.org/api/#!/Templates/post_templates
    val templateId = if (!uploadToCedar) {
      scribe.info("--upload-cedar is false, so not uploading to CEDAR workbench.")
      None
    } else {
      val response = requests.post(
        "https://resource.metadatacenter.org/templates",
        data = pretty(render(cedarTemplate)),
        params =
          if (cedarUploadFolderURL.isEmpty) Map()
          else Map("folder_id" -> cedarUploadFolderURL.head),
        headers = Map("Authorization" -> s"apiKey $apiKey"),
        check = false // Don't throw exceptions on HTTP error -- let us handle it.
      )
      if (response.statusCode != 201) {
        scribe.error(s"Could not upload template: ${response.statusCode} ${response.statusMessage}")
        scribe.error(s"Content: ${pretty(render(parse(response.text())))}")
        return
      }
      val generatedTemplateId = (parse(response.text()) \ "@id").extract[String]
      scribe.info(s"Template successfully uploaded as $generatedTemplateId.")
      Some(generatedTemplateId)
    }

    // Step 2. Create a CEDAR Instance for each row in the input data.
    // We start with the base CEDAR instance -- the information that appears to be identical for each CEDAR instance.
    val baseCEDARInstance =
      ("@context" ->
        // Prefixes
        ("skos" -> "http://www.w3.org/2004/02/skos/core#") ~
          ("pav" -> "http://purl.org/pav/") ~
          ("rdfs" -> "http://www.w3.org/2000/01/rdf-schema#") ~
          ("schema" -> "http://schema.org/") ~
          ("oslc" -> "http://open-services.net/ns/core#") ~
          ("xsd" -> "http://www.w3.org/2001/XMLSchema#") ~

          // CEDAR Template metadata
          ("skos:notation" -> ("@type" -> "xsd:string")) ~
          ("pav:derivedFrom" -> ("@type" -> "@id")) ~
          ("pav:createdOn" -> ("@type" -> "xsd:dateTime")) ~
          ("pav:lastUpdatedOn" -> ("@type" -> "xsd:dateTime")) ~
          ("oslc:modifiedBy" -> ("@type" -> "@id")) ~
          ("schema:isBasedOn" -> ("@type" -> "@id")) ~
          ("schema:description" -> ("@type" -> "xsd:string")) ~
          ("schema:name" -> ("@type" -> "xsd:string")) ~
          ("pav:createdBy" -> ("@type" -> "@id")) ~

          // Commonly used fields.
          ("id" -> "http://purl.org/dc/terms/identifier") ~
          ("rdfs:label" -> ("@type" -> "xsd:string")))

    // Generate a CEDAR instance for each row in the input data.
    dataWithHeaders.zipWithIndex
      .foreach({
        case (row, index) =>
          val values: Seq[JField] = headerRow
            .map(colName => (colName, row.get(colName)))
            .map({
              case (colName, Some(value)) => {
                // Load the property information for this column.
                val prop = properties.getOrElse(colName, JObject(List()))

                val mappings: Seq[JObject] = (prop \ "enumValues") match {
                  case JArray(enumValues) => {
                    enumValues flatMap { enumValue =>
                      val mappingValue = (enumValue \ "value").extract[String]
                      // scribe.info(s"Comparing '$value' with '$mappingValue'")
                      if (value == mappingValue) {
                        // Gotta map!
                        val uri = (enumValue \ "conceptURI").extract[String]
                        val caDSRValue = (enumValue \ "caDSRValue").extract[String]

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

                // Convert this cell in the input data into a JField. The JField includes both the field name (in this
                // case, the column name) and the value (a representation of the value as a JSON-LD value.
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
                      case JString("number") => None
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

          // Create a CEDAR instance by adding metadata and data values to the base CEDAR instance.
          val cedarInstance: json4s.JObject = baseCEDARInstance ~
            ("schema:name" -> s"csv2caDSR CEDAR Instance Export ($pavCreatedOn) for row $index") ~
            ("schema:description" -> s"csv2caDSR CEDAR Instance Export ($pavCreatedOn) for row $index from $inputCSVFile") ~
            ("schema:isBasedOn" -> templateId.getOrElse("")) ~
            ("pav:createdBy" -> pavCreatedBy) ~
            ("pav:createdOn" -> pavCreatedOn) ~
            ("pav:lastUpdatedOn" -> pavCreatedOn) ~
            ("oslc:modifiedBy" -> pavCreatedBy) merge
            JObject(values.toList)

          // Write out "$cedarBasename.instance.$index.json".
          val templateFilename = new File(cedarBasename.getAbsolutePath + s".instance.$index.json")
          val templateWriter = new FileWriter(templateFilename)
          templateWriter.append(pretty(render(cedarInstance)))
          templateWriter.close()

          // Optionally, publish instance to CEDAR Workbench.
          if (!uploadToCedar) {
            scribe.info("--upload-cedar is false, so not uploading instances to CEDAR workbench.")
            None
          } else {
            val response = requests.post(
              "https://resource.metadatacenter.org/template-instances",
              data = pretty(render(cedarInstance)),
              params =
                if (cedarUploadFolderURL.isEmpty) Map()
                else Map("folder_id" -> cedarUploadFolderURL.head),
              headers = Map("Authorization" -> s"apiKey $apiKey"),
              check = false // Don't throw exceptions on HTTP error -- let us handle it.
            )
            if (response.statusCode != 201) {
              scribe.error(
                s"Could not upload instance based on $templateId: ${response.statusCode} ${response.statusMessage}"
              )
              scribe.error(s"Content: ${pretty(render(parse(response.text())))}")
            } else {

              val generatedInstanceId = (parse(response.text()) \ "@id").extract[String]
              scribe.info(s"Template instance successfully uploaded as $generatedInstanceId.")
            }
          }
      })
  }
}
