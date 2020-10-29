package org.renci.ccdh.csv2cadsr.schema

import java.net.URI

import org.json4s._

import scala.util.Try

/**
  * A MappingSchema records all the information needed to map an input CSV file into the Portable Format
  * for Biomedical Data (PFB) and/or CEDAR element formats.
  *
  * There are three types of data we want to capture in this JSON Schema:
  *  - Basic metadata: field name, description field, and so on.
  *  - Semantic mapping: what does this field mean? Can it be represented by a caDSR value?
  *  - JSON Schema typing information: is it a string, number, enum? What are the enumerated values?
  *  - Value mapping: how can the enumerated values be mapped to concepts?
  */
case class MappingSchema(fields: Seq[MappingField]) {
  def asJsonSchema: JObject = {
    val fieldEntries = fields map { field => (field.name, field.asJsonSchema) }

    JObject(
      "properties" -> JObject(fieldEntries.toList),
      "required" -> JArray(fields.filter(_.required).map(_.name).map(JString).toList)
    )
  }
}

object MappingSchema {
  val empty = MappingSchema(Seq())
}

/**
  * A MappingField contains information on each field in a mapping.
  */
abstract class MappingField(
  val name: String,
  val uniqueValues: Set[String],
  val required: Boolean = false
) {
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values)"
  }

  def asJsonSchema: JObject
}

object MappingField {

  /**
    * If the number of unique values are less than this proportion of all values,
    * we consider this field to be an enum.
    */
  final val enumProportion = 0.1

  def createFromValues(name: String, values: Seq[String]): MappingField = {
    val uniqueValues = values.toSet

    // Mark this property as required if we have no blanks in the values.
    val isRequired = !(values exists { v => v.isBlank })

    uniqueValues match {
      case _ if (uniqueValues.isEmpty) => EmptyField(name, isRequired)
      case _ if (uniqueValues.size < values.size * enumProportion) => {
        val uniqueValuesByCount = values
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

/*
 * We support Required fields.
 */
class Required {
  def isRequired: Boolean = true
}

/*
 * We support different kinds of mapping fields.
 */
case class StringField(
  override val name: String,
  override val uniqueValues: Set[String],
  override val required: Boolean = false
) extends MappingField(name, uniqueValues) {
  override def asJsonSchema: JObject =
    JObject(
      "type" -> JString("string"),
      "description" -> JString(""),
      "caDSR" -> JString(""),
      "caDSRVersion" -> JString("1.0")
    )
}
case class EnumField(
  override val name: String,
  override val uniqueValues: Set[String],
  val values: Seq[EnumValue] = Seq(),
  override val required: Boolean = false
) extends MappingField(name, uniqueValues, required) {
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values: ${uniqueValues.mkString(", ")})"
  }

  override def asJsonSchema: JObject =
    JObject(
      "type" -> JString("string"),
      "description" -> JString(""),
      "caDSR" -> JString(""),
      "caDSRVersion" -> JString("1.0"),
      "permissibleValues" -> JArray(List()),
      "enum" -> JArray(values.map(_.value).map(JString).toList),
      "enumValues" -> JArray(values.map(_.asMapping).toList)
    )
}
case class EnumValue(val value: String, val conceptURI: Option[URI] = None) {
  def asMapping: JObject =
    JObject(
      "value" -> JString(value),
      "description" -> JString(""),
      "caDSRValue" -> JString(""),
      "conceptURI" -> JString(conceptURI map (_.toString) getOrElse (""))
    )
}
object EnumValue {
  def fromString(value: String): EnumValue = {
    // Does the value contain a URI?
    val uriRegex = "(https?://\\S+)".r
    val result =
      uriRegex findAllIn (value) map (m => Try { new URI(m) }) filter (_.isSuccess) map (_.get)
    // If so, use the last URI as the conceptURI.
    EnumValue(value, result.toSeq.lastOption)
  }
}
case class IntField(
  override val name: String,
  override val uniqueValues: Set[String],
  override val required: Boolean,
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
  override def asJsonSchema: JObject =
    JObject(
      "type" -> JString("string"),
      "description" -> JString(""),
      "caDSR" -> JString(""),
      "caDSRVersion" -> JString("1.0")
    )
}
