package org.renci.ccdh.csv2cadsr

import java.io.{BufferedWriter, File, FileWriter}

import com.github.tototoshi.csv.CSVReader
import org.json4s.{JObject, JValue, StringInput}

import scala.io.Source
import org.json4s.native.Serialization.writePretty
import org.json4s.native.JsonMethods.parse
import caseapp._
import org.json4s

@AppName("csv2caDSR")
@AppVersion("0.1.0")
@ProgName("csv2caDSR")
case class CommandLineOptions(
  @HelpMessage("The CSV data file to read from.")
  csv: Option[String],
  @HelpMessage("The JSON mapping file to read from.")
  json: Option[String],
  @HelpMessage("The JSON mapping file to write to.")
  toJson: Option[String],
  @HelpMessage("The CSV file to write harmonized data to.")
  toCsv: Option[String],
  @HelpMessage("The PFB file to write harmonized data to.")
  toPfb: Option[String],
  @HelpMessage("The filename prefix used to write harmonized data as CEDAR instance data")
  toCedar: Option[String],
  @HelpMessage("Should we attempt to upload the harmonized data to CEDAR? (requires a ~/.cedar.properties file with an apiKey property)")
  uploadToCedar: Boolean = false,
  @HelpMessage("If uploaded to the CEDAR workbench, specify the folder that it should be uploaded to")
  cedarUploadFolderUrl: Option[String] = None
)

/**
  * csv2caDSR is a tool for harmonizing CSV data against the
  * Cancer Data Standards Registry and Repository (caDSR).
  * Its behavior depends on the combination of input and output
  * files provided:
  *  - If called with `--csv filename.csv --to-json output.json`,
  *    write out the column information of this file as a JSON file.
  *  - If called with `--json filename.json --to-json filename.json`,
  *    fill out information from the caDSR. Two possible kinds of
  *    information can be filled out here:
  *     - 1. Field-level information, such as a description, list of
  *          permissible values, and so on.
  *     - 2. Value-level information, such as its description, concept
  *          identifier, and so on.
  *  - If called with `--csv filename.csv --json filename.json --to-csv output.csv`,
  *    we use the mapping information in filename.json to harmonize the input dataset
  *    filename.csv and to write it out as a CSV file to output.csv.
  */
object csv2caDSR extends CaseApp[CommandLineOptions] {
  // Set up a default format for JSON import/export.
  implicit val formats = org.json4s.DefaultFormats

  /**
    * Given an input CSV file, produce an output JSON file that describes the columns in that file.
    *
    * @param csvInputFile The CSV input file.
    * @param jsonOutputFile The JSON file to write to.
    */
  def generateJSONFile(csvInputFile: File, jsonOutputFile: File): Unit = {
    val bufferedWriter = new BufferedWriter(new FileWriter(jsonOutputFile))
    val result = schema.MappingSchema.generateFromCsv(csvInputFile)
    result.fold(
      throwable => scribe.error(s"Could not generate JSON Schema: ${throwable}"),
      result => bufferedWriter.write(writePretty(result.asJsonSchema))
    )
    bufferedWriter.close()
    scribe.info(s"Wrote out an empty JSON mapping file to ${jsonOutputFile}")
  }

  /**
    * Given an input JSON file, produce an output JSON file that fills in some of the fields in the JSON file.
    *
    * @param jsonInputFile The JSON input file.
    * @param jsonOutputFile The JSON file to write to.
    */
  def fillJSONFile(jsonInputFile: File, jsonOutputFile: File): Unit = {
    // Fill in the JSON mappings with information from the caDSR system.
    val jsonSource: Source = Source.fromFile(jsonInputFile)("UTF-8")
    val bufferedWriter = new BufferedWriter(new FileWriter(jsonOutputFile))

    val jsonMappings = parse(StringInput(jsonSource.getLines().mkString("\n")))
    val filledScheme = schema.JSONMappingFiller.fillProperties(jsonMappings \ "properties")
    bufferedWriter.write(writePretty(filledScheme))
    bufferedWriter.close()
    scribe.info(s"Wrote out filled-in JSON mapping file to ${jsonOutputFile}")
  }

  /** Export the harmonized data, based on the command line options selected. */
  def exportHarmonizedData(csvFile: File, jsonFile: File, options: CommandLineOptions) = {
    // Load the JSON mappings file.
    val jsonSource: Source = Source.fromFile(jsonFile)("UTF-8")
    val jsonRoot: JValue = parse(StringInput(jsonSource.getLines().mkString("\n")))

    val properties: Map[String, json4s.JValue] = (jsonRoot \ "properties") match {
      case obj: JObject => obj.obj.toMap
      case _            => throw new RuntimeException("JSON source is not a JSON object")
    }

    // Load the CSV data file.
    val csvSource: Source = Source.fromFile(csvFile)("UTF-8")

    // Look through the command line options to see how we should export our data.
    options match {
      case opt if opt.toJson.nonEmpty => {
        // TODO: implement our own JSON-LD export for this data.
        ???
      }

      case opt if opt.toCsv.nonEmpty => {
        // Generate the CSV!
        val csvOutputFile = opt.toCsv.map(new File(_)).head
        val reader = CSVReader.open(csvSource)
        val bufferedWriter = new BufferedWriter(new FileWriter(csvOutputFile))
        output.ToCSV.write(reader, properties, bufferedWriter)
        bufferedWriter.close()
        scribe.info(s"Wrote out CSV harmonized output file to ${csvOutputFile}")
      }

      case opt if opt.toPfb.nonEmpty => {
        // Generate the PFB!
        val pfbOutputFile = opt.toPfb.map(new File(_)).head
        val reader = CSVReader.open(csvSource)
        output.ToPFB.writePFB(reader, properties, pfbOutputFile)
        scribe.info(s"Wrote output as PFB file to ${pfbOutputFile}.")
      }

      case opt if opt.toCedar.nonEmpty => {
        // Generate the CEDAR instance data!
        val cedarPrefix = opt.toCedar.map(new File(_)).head
        val csvReader = CSVReader.open(csvSource)
        val uploadToCedar = opt.uploadToCedar
        val cedarFolderURL = opt.cedarUploadFolderUrl
        output.ToCEDAR.writeCEDAR(csvFile, csvReader, properties, cedarPrefix, uploadToCedar, cedarFolderURL)
        scribe.info(s"Wrote output as CEDAR file to prefix ${cedarPrefix}.")
      }

      case _ =>
        throw new RuntimeException(
          "No output format provided. Use --help to see a list of output formats (--to-*)."
        )
    }
  }

  /**
    * Look through the command line options and figure out what the user wants to do.
    */
  def run(options: CommandLineOptions, args: RemainingArgs): Unit = {
    val csvFile: Option[File] = options.csv.map(new File(_))
    val jsonFile: Option[File] = options.json.map(new File(_))
    val jsonOutputFile: Option[File] = options.toJson.map(new File(_))

    if (jsonFile.nonEmpty && csvFile.nonEmpty) {
      // Export the harmonized data.
      exportHarmonizedData(csvFile.get, jsonFile.get, options)

    } else if (jsonFile.nonEmpty && csvFile.isEmpty) {
      // Fill in the JSON filename.
      if (jsonOutputFile.isEmpty)
        throw new RuntimeException(
          s"Could not fill JSON file ${jsonFile}: no output JSON file specified (use --to-json filename.json)"
        )

      fillJSONFile(jsonFile.get, jsonOutputFile.get)
    } else if (jsonFile.isEmpty && csvFile.nonEmpty) {
      // Write out a JSON file that describes the CSV file.
      if (jsonOutputFile.isEmpty)
        throw new RuntimeException(
          s"Could not write JSON file from CSV input ${csvFile}: no output JSON file specified (use --to-json filename.json)"
        )

      generateJSONFile(csvFile.get, jsonOutputFile.get)
    } else {
      // No inputs provided.
      throw new RuntimeException(
        "Cannot run: need a CSV input file (--csv input.csv) and/or a JSON input file (--json input.json)"
      )
    }
  }
}
