package org.renci.ccdh.csv2cadsr

import java.io.{BufferedWriter, FileWriter, OutputStreamWriter, File}

import com.github.tototoshi.csv.CSVReader
import org.json4s.{DefaultFormats, JObject, JValue, StringInput}

import scala.io.Source
import org.json4s.native.Serialization.writePretty
import org.json4s.native.JsonMethods._
import caseapp._

import scala.collection.immutable.HashMap

@AppName("csv2caDSR")
@AppVersion("0.1.0")
@ProgName("csv2caDSR")
case class CommandLineOptions(
  @HelpMessage("The CSV data file to read.")
  csv: Option[String],

  @HelpMessage("The JSON file to read.")
  json: Option[String],

  @HelpMessage("The JSON file to write.")
  toJson: Option[String],

  @HelpMessage("Write output in CSV to the provided file")
  toCsv: Option[String]
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
  implicit val formats = DefaultFormats

  /**
    * Given an input CSV file, produce an output JSON file that describes the columns in that file.
    *
    * @param csvInputFile The CSV input file.
    * @param jsonOutputFile The JSON file to write to.
    */
  def generateJSONFile(csvInputFile: File, jsonOutputFile: File): Unit = {
    val csvSource = Source.fromFile(csvInputFile)("UTF-8")
    val bufferedWriter = new BufferedWriter(new FileWriter(jsonOutputFile))
    val result = schema.MappingGenerator.generateFromCsv(csvSource)
    result.fold(
      throwable => scribe.error(s"Could not generate JSON Schema: ${throwable}"),
      result => bufferedWriter.write(writePretty(result.asJsonSchema))
    )
  }

  /**
    * Given an input JSON file, produce an output JSON file that fills in some of the fields in the JSON file.
    *
    * @param jsonInputFile The JSON input file.
    * @param jsonOutputFile The JSON file to write to.
    */
  def fillJSONFile(jsonInputFile: File, jsonOutputFile: File): Unit = {
    // This is a hack to fill in the JSON Schema with information from the caDSR system.
    val jsonSource: Source = Source.fromFile(jsonInputFile)("UTF-8")
    val bufferedWriter = new BufferedWriter(new FileWriter(jsonOutputFile))

    val filledScheme = schema.Filler.fill(parse(StringInput(jsonSource.getLines().mkString("\n"))))
    bufferedWriter.write(writePretty(filledScheme))
  }

  /**
    * Look through the command line options and figure out what the user wants to do.
    */
  def run(options: CommandLineOptions, args: RemainingArgs): Unit = {
    val csvFilename: Option[File] = options.csv.map(new File(_))
    val jsonFilename: Option[File] = options.json.map(new File(_))
    val jsonOutputFile: Option[File] = options.toJson.map(new File(_))
    val csvOutputFile: Option[File] = options.toCsv.map(new File(_))

    if (jsonFilename.nonEmpty && csvFilename.nonEmpty) {
      // Export in some way. We will need to load the JSON file.
      val jsonSource: Source = Source.fromFile(jsonFilename.get)("UTF-8")
      val jsonRoot = parse(StringInput(jsonSource.getLines().mkString("\n")))
      val properties: Map[String, JValue] = jsonRoot match {
        case obj: JObject =>
          obj.values.getOrElse("properties", HashMap()).asInstanceOf[HashMap[String, JValue]]
        case _ => throw new RuntimeException("JSON source is not a JSON object")
      }

      // Determine which type of output the user wants.
      if (csvOutputFile.nonEmpty) {
        // We have a JSON schema file and a CSV file.
        val csvSource: Source = Source.fromFile(csvFilename.get)("UTF-8")

        // Generate the CSV!
        val reader = CSVReader.open(csvSource)
        val bufferedWriter = new BufferedWriter(new FileWriter(csvOutputFile.get))
        output.ToCSV.write(
          reader,
          properties,
          bufferedWriter
        )
      } else if(jsonOutputFile.nonEmpty) {
        // TODO: export data as JSON-LD.

      } else {
        throw new RuntimeException("No output format provided. Use --help to see a list of output formats (--to-*).")
      }

    } else if (jsonFilename.nonEmpty && csvFilename.isEmpty) {
      // Fill in the JSON filename.
      if (jsonOutputFile.isEmpty) throw new RuntimeException(s"Could not fill JSON file ${jsonFilename}: no output JSON file specified (use --to-json filename.json)")

      generateJSONFile(csvFilename.get, jsonOutputFile.get)
    } else if (jsonFilename.isEmpty && csvFilename.nonEmpty) {
      // Write out a JSON file that describes the CSV file.
      if (jsonOutputFile.isEmpty) throw new RuntimeException(s"Could not write JSON file from CSV input ${csvFilename}: no output JSON file specified (use --to-json filename.json)")

      // TODO
    } else {
      // No inputs provided.
      throw new RuntimeException("Cannot run: need a CSV input file (--csv input.csv) and/or a JSON input file (--json input.json)")
    }
  }
}
