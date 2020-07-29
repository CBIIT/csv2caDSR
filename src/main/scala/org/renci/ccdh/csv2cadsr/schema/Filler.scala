package org.renci.ccdh.csv2cadsr.schema

import org.json4s.JsonAST.{JField, JObject, JString, JValue}
import org.json4s.native.JsonMethods._

import scala.io.Source

/**
 * The Filler fills in a schema based on the provided properties.
 *
 * For now, we only resolve caDSRs, but we could imagine doing this
 * with subclasses of ontology terms and so on.
 */
object Filler {
  def fill(root: JValue): JValue = root.mapField({
    case ("properties", props: JObject) => ("properties", fillProperties(props))
    case other => other
  })

  def fillProperties(props: JValue): JValue = props.mapField({
    case (propName, property: JObject) => (propName, fillProperty(propName, property))
    case other => other
  })

  def fillProperty(str: String, value: JObject): JObject = {
    val element = (value \ "caDSR")
    element match {
      case JString(caDSR) => fillPropertyFromCaDSR(str, value, caDSR)
      case _ => value
    }
  }

  def fillPropertyFromCaDSR(fieldName: String, value: JObject, caDSR: String): JObject = {
    scribe.info(s"Retrieving caDSR $caDSR.")

    val caDSRContent = Source.fromURL(s"https://fhir.hotecosystem.org/terminology/cadsr/ValueSet/$caDSR?_format=json")
    val caDSRJson = parse(caDSRContent.mkString(""))

    JObject(value.obj.mapConserve({
      case ("description", JString("")) => ("description", caDSRJson \ "description")
      case other => other
    }))
  }
}
