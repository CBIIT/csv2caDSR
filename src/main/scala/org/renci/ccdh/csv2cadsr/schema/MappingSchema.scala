package org.renci.ccdh.csv2cadsr.schema

import java.io.File
import java.net.URI

import com.github.tototoshi.csv.CSVReader
import org.json4s._

import scala.io.Source
import scala.util.{Success, Try}

import org.json4s.JsonDSL.WithBigDecimal._

/**
  * A MappingSchema records all the information needed to map an input CSV file into the Portable Format
  * for Biomedical Data (PFB) and/or CEDAR element formats. It consists of a sequence of MappingFields
  * (described below).
  *
  * There are three types of data we want to capture in this JSON Schema:
  *  - Basic metadata: field name, description field, and so on.
  *  - Semantic mapping: what does this field mean? Can it be represented by a caDSR value?
  *  - JSON Schema typing information: is it a string, number, enum? What are the enumerated values?
  *  - Value mapping: how can the enumerated values be mapped to concepts?
  *
  * Note that all the caDSR-related fields are left blank here. The intention is that we generate a JSON mapping file
  * with blank descriptions/caDSR CDE ID and so on, and the user will fill them in before mapping data.
  */
case class MappingSchema(fields: Seq[MappingField]) {
  def asJsonSchema: JObject = {
    val fieldEntries = fields map { field => (field.name, field.asJsonObject) }

    JObject(
      // List of all the properties.
      "properties" -> JObject(fieldEntries.toList),
      // Properties named in this list are required.
      "required" -> JArray(fields.filter(_.required).map(_.name).map(JString).toList)
    )
  }
}

/**
  * Some static methods for MappingSchema.
  */
object MappingSchema {
  /** An empty mapping schema. */
  val empty = MappingSchema(Seq())

  /**
    * Try to generate a mapping schema from a CSV file.
    *
    * @param csvFile A CSV file to load.
    * @return Either Success(aMappingSchema) or Failure(Exception).
    */
  def generateFromCsv(csvFile: File): Try[MappingSchema] = {
    // Step 1. Load data from CSV file.
    val csvSource = Source.fromFile(csvFile, "UTF-8")
    val reader = CSVReader.open(csvSource)
    val (headerRow, dataWithHeaders) = reader.allWithOrderedHeaders()

    // Headers are in the first row.
    val fields = headerRow map { fieldName =>
      // Use the values in this column to try to guess its mapping field type.
      MappingField.createFromValues(fieldName, dataWithHeaders.flatMap(_.get(fieldName)))
    }

    Success(MappingSchema(fields))
  }
}

/**
  * A MappingField contains information on each field in a mapping. It is the parent class
  * of all mapping fields.
  */
abstract class MappingField(
  /** The name of this field */
  val name: String,
  /** The set of unique values that we observe in this field. */
  val uniqueValues: Set[String],
  /** Whether this field appears to be required. */
  val required: Boolean = false
) {

  /** Provides a default toString method for mapping fields. */
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values)"
  }

  /** Return a JSON object describing this mapping field. */
  def asJsonObject: JObject
}

/**
  * Some static methods for generating mapping fields.
  */
object MappingField {
  /**
    * If the number of unique values are less than this proportion of all values,
    * we consider this field to be an enum.
    */
  final val enumProportion = 0.1

  /**
    * Create a field that represents a column name and a particular sequence of values.
    */
  def createFromValues(name: String, values: Seq[String]): MappingField = {
    // Which unique values are found in this field?
    val uniqueValues = values.toSet

    // Mark this property as required if we have no blanks in the values.
    val isRequired = !(values exists { v => v.isBlank })

    uniqueValues match {
      // Is it an empty field?
      case _ if (uniqueValues.isEmpty) => EmptyField(name, isRequired)

      // Is it an enum field?
      case _ if (uniqueValues.size < values.size * enumProportion) => {
        // Create an ordered list of the possible values, with the most frequently repeated value appearing first.
        val uniqueValuesByCount: Seq[(String, Int)] = values
          .groupBy(identity)
          .transform((_, v) => v.size)
          .toSeq
          .sortBy(_._2)(Ordering[Int].reverse)
        EnumField(
          name,
          uniqueValues,
          uniqueValuesByCount.map(_._1).map(EnumValue.fromString),
          isRequired
        )
      }
      case _ if (uniqueValues forall { str => str forall { ch: Char => Character.isDigit(ch) || ch == '-' }}) => {
        val intValues = values flatMap (_.toIntOption)
        IntField(name, uniqueValues, isRequired, Range.inclusive(intValues.min, intValues.max))
      }
      case _ if (uniqueValues forall { str => str forall { ch: Char => Character.isDigit(ch) || ch == '-' || ch == '.' }}) => {
        val numValues = values flatMap (v => Try(BigDecimal(v)).toOption)
        NumberField(name, uniqueValues, isRequired, numValues.min, numValues.max)
      }
      case _ => StringField(name, uniqueValues, isRequired)
    }
  }
}

/**
  * String fields are the most basic kind of field: they include string values, and don't put any constraint on them.
  */
case class StringField(
  override val name: String,
  override val uniqueValues: Set[String],
  override val required: Boolean = false
) extends MappingField(name, uniqueValues) {
  override def asJsonObject: JObject =
    ("type" -> "string") ~
    ("description" -> "") ~
    ("caDSR" -> "") ~
    ("caDSRVersion" -> "1.0")
}

/**
  * Enum fields can have one of a set of values (modeled by EnumValues).
  */
case class EnumField(
  override val name: String,
  override val uniqueValues: Set[String],
  /** The EnumValues in this EnumField. */
  enumValues: Seq[EnumValue] = Seq(),
  override val required: Boolean = false
) extends MappingField(name, uniqueValues, required) {
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values: ${uniqueValues.mkString(", ")})"
  }

  override def asJsonObject: JObject =
    ("type" -> "string") ~
    ("description" -> "") ~
    ("caDSR" -> "") ~
    ("caDSRVersion" -> "1.0") ~
    ("permissibleValues" -> JArray(List())) ~
    ("enum" -> enumValues.map(_.value).map(JString).toList) ~
    ("enumValues" -> enumValues.map(_.asMapping).toList)
}

/** An EnumValue represents one possible value for an EnumField. */
case class EnumValue(value: String, conceptURI: Option[URI] = None) {
  def asMapping: JObject =
    ("value" -> value) ~
    ("description" -> "") ~
    ("caDSRValue" -> "") ~
    ("conceptURI" -> (conceptURI map (_.toString) getOrElse ("")))
}

/** Some static methods for EnumValues. */
object EnumValue {
  /** Convert a string value to an EnumValue. */
  def fromString(value: String): EnumValue = {
    // Does the value contain an HTTP or HTTPS URL?
    val uriRegex = "(https?://\\S+)".r
    val result =
      uriRegex findAllIn (value) map (m => Try { new URI(m) }) filter (_.isSuccess) map (_.get)
    // If so, use the last URI as the conceptURI.
    EnumValue(value, result.toSeq.lastOption)
  }
}

/** An IntField models a field that can only contain integer values. */
case class IntField(
  override val name: String,
  override val uniqueValues: Set[String],
  override val required: Boolean,
  /** The smallest and largest integers in this field. */
  range: Range
) extends MappingField(name, uniqueValues) {
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values in ${range})"
  }
  override def asJsonSchema: JObject =
    JObject(
      "type" -> JString("integer"),
      "description" -> JString(""),
      "caDSR" -> JString(""),
      "caDSRVersion" -> JString("1.0")
    )
}

case class NumberField(
  override val name: String,
  override val uniqueValues: Set[String],
  override val required: Boolean,
  min: BigDecimal,
  max: BigDecimal
) extends MappingField(name, uniqueValues) {
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values between ${min} and ${max})"
  }

  override def asJsonSchema: JObject =
    JObject(
      "type" -> JString("number"),
      "description" -> JString(""),
      "caDSR" -> JString(""),
      "caDSRVersion" -> JString("1.0")
    )
}

case class EmptyField(override val name: String, override val required: Boolean)
    extends MappingField(name, Set()) {
  override def asJsonObject: JObject =
    ("type" -> "string") ~
    ("description" -> "") ~
    ("caDSR" -> "") ~
    ("caDSRVersion" -> "1.0")
}
