package org.renci.ccdh.csv2cadsr

import java.io.{BufferedWriter, File, FileWriter, OutputStreamWriter}

import com.github.tototoshi.csv.CSVReader
import org.json4s.{DefaultFormats, JObject, JValue, StringInput}

import scala.io.Source
import org.json4s.native.Serialization.writePretty
import org.json4s.native.JsonMethods._
import caseapp._

import scala.collection.immutable.HashMap

@AppName("csv2caDSR")
@AppVersion("0.1.0")
@ProgName("csv2cadsr")
case class CommandLineOptions(
  @ValueDescription("An optional file to write output to")
  @ExtraName("o")
  outputFilename: Option[String],

  @ValueDescription("Write to the specified PFB file")
  toPfb: Option[String]
)

object csv2caDSR extends CaseApp[CommandLineOptions] {
  def run(options: CommandLineOptions, args: RemainingArgs) = {
    val csvFilename: String = args.remaining.head
    val jsonFilename: Option[String] = args.remaining.tail.headOption
    val bufferedWriter = options.outputFilename match {
      case Some(filename: String) => new BufferedWriter(new FileWriter(filename))
      case _ => new BufferedWriter(new OutputStreamWriter(System.out))
    }
    val pfbFilename: Option[File] = options.toPfb.map(new File(_))

    implicit val formats = DefaultFormats

    if (jsonFilename.isEmpty) {
      // No JSON file provided? Then generate the JSON file!
      val csvSource: Source = Source.fromFile(csvFilename)("UTF-8")

      val result = schema.MappingGenerator.generateFromCsv(csvSource)
      result.fold(
        throwable => scribe.error(s"Could not generate JSON Schema: ${throwable}"),
        result => bufferedWriter.write(writePretty(result.asJsonSchema))
      )
      scribe.info("Wrote out JSON schema.")
    } else if (csvFilename == "fill") {
      // This is a hack to fill in the JSON Schema with information from the caDSR system.
      val jsonSource: Source = Source.fromFile(jsonFilename.get)("UTF-8")

      val filledScheme = schema.Filler.fill(parse(StringInput(jsonSource.getLines().mkString("\n"))))
      bufferedWriter.write(writePretty(filledScheme))
      scribe.info("Wrote out filled JSON schema.")
    } else {
      // We have a JSON schema file and a CSV file. Generate the JSON!
      val csvSource: Source = Source.fromFile(csvFilename)("UTF-8")
      val jsonSource: Source = Source.fromFile(jsonFilename.get)("UTF-8")

      val jsonRoot = parse(StringInput(jsonSource.getLines().mkString("\n")))
      val properties: Map[String, JValue] = jsonRoot match {
        case obj: JObject =>
          obj.values.getOrElse("properties", HashMap()).asInstanceOf[HashMap[String, JValue]]
        case _ => throw new RuntimeException("JSON source is not a JSON object")
      }

      val reader = CSVReader.open(csvSource)

      if (pfbFilename.nonEmpty) {
        output.ToPFB.writePFB(
          reader,
          properties,
          pfbFilename.get
        )
        scribe.info(s"Wrote output as PFB file to ${pfbFilename}.")
      } else {
        // Default to CSV.
        output.ToCSV.write(
          reader,
          properties,
          bufferedWriter
        )
        scribe.info("Wrote output as CSV file.")
      }
    }

    bufferedWriter.close()
  }
}
