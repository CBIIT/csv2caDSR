package org.renci.ccdh.csv2cadsr.schema

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

}

object MappingSchema {
  val empty = MappingSchema(Seq())
}

/**
 * A MappingField contains information on each field in a mapping.
 */
abstract class MappingField(name: String, uniqueValues: Set[String]) {
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values)"
  }
}

object MappingField {
  /**
   * If the number of unique values are less than this proportion of all values,
   * we consider this field to be an enum.
   */
  final val enumProportion = 0.1

  def createFromValues(name: String, exampleValues: Seq[String]): MappingField = {
    val uniqueExampleValues = exampleValues.toSet
    val result = uniqueExampleValues match {
      case _ if (uniqueExampleValues.isEmpty) => EmptyField(name)
      case _ if (uniqueExampleValues.size < exampleValues.size * enumProportion) => EnumField(name, uniqueExampleValues)
      case _ if (uniqueExampleValues forall {str => str forall Character.isDigit}) => {
        val values = exampleValues flatMap (_.toIntOption)
        IntField(name, uniqueExampleValues, Range.inclusive(values.min, values.max))
      }
      case _ => StringField(name, uniqueExampleValues)
    }

    result
  }
}

/*
 * We support different kinds of mapping fields.
 */
case class StringField(name: String, uniqueValues: Set[String]) extends MappingField(name, uniqueValues)
case class EnumField(name: String, uniqueValues: Set[String]) extends MappingField(name, uniqueValues) {
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values: ${uniqueValues.mkString(", ")})"
  }
}
case class IntField(name: String, uniqueValues: Set[String], range: Range) extends MappingField(name, uniqueValues) {
  override def toString: String = {
    s"${getClass.getSimpleName}(${name} with ${uniqueValues.size} unique values in ${range})"
  }
}
case class EmptyField(name: String) extends MappingField(name, Set())