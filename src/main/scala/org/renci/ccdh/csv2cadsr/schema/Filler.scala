package org.renci.ccdh.csv2cadsr.schema

import java.net.URI

import org.json4s
import org.json4s.JsonAST.{JArray, JField, JObject, JString, JValue}
import org.json4s.{JNothing, JsonDSL}
import org.json4s.native.JsonMethods._

import scala.io.Source
import scala.xml.XML

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

  def queryCaDSRAPI(query: String): String = {
    val valueDomainURI = new URI(
      s"https",
      "cadsrapi.nci.nih.gov",
      "/cadsrapi41/GetXML",
      query,
      ""
    )
    val connection = valueDomainURI.toURL.openConnection()
    connection.connect()
    val result = Source
      .fromInputStream(
        connection.getInputStream,
        if (connection.getContentEncoding != null) connection.getContentEncoding else "UTF-8"
      )
      .getLines()
      .mkString("\n")

    scribe.debug(s"Queried caDSR API with '$query', retrieved output: $result\n===")

    result
  }

  def fillPropertyFromCaDSR(fieldName: String, value: JObject, caDSR: String): JObject = {
    if (caDSR.isEmpty) return value

    scribe.info(s"Retrieving caDSR $caDSR.")

    val caDSRContent = Source.fromURL(s"https://fhir.hotecosystem.org/terminology/cadsr/ValueSet/$caDSR?_format=json")
    val caDSRJson = parse(caDSRContent.mkString(""))
    val caDSRPermissibleValues = (caDSRJson \ "expansion" \ "contains").toOption match {
      case None => JArray(List())
      case Some(arr: JArray) => arr
      case _ => throw new Error(s".expansion.contains in JSON request is not an array as expected: ${caDSRJson}")
    }
    val caDSRPVCodeMap = caDSRPermissibleValues.arr.map({
      case codeEntry: JObject => (codeEntry.values.getOrElse("code", "(none)"), codeEntry)
    }).toMap

    JObject(value.obj.mapConserve({
      case ("description", JString("")) => ("description", caDSRJson \ "description")
      case ("permissibleValues", JArray(List())) => ("permissibleValues", JArray(caDSRPVCodeMap.keys.map(key => JString(key.toString)).toList))
      case ("enumValues", enumValues: JArray) => ("enumValues", JArray(enumValues.arr.mapConserve({
        case enumValue: JObject => {
          val caDSRValue: String = enumValue.values.getOrElse("caDSRValue",
            // If not caDSRValue is provided, just use the original value.
            enumValue.values.getOrElse("value", "")
          ).toString
          // Is there a mapping for the caDSR value?
          val lookup = caDSRPVCodeMap.get(caDSRValue)
          if (lookup.isEmpty) {
            // No lookup found.
            enumValue
          } else {
            // Found a match!
            val coding = (lookup.get.findField(_._1 == "coding").map(_._2))
            scribe.info(s"Coding: $coding")
            coding match {
              case Some(arr: JArray) => JObject(enumValue.obj.mapConserve({
                  case ("description", JString("")) => ("description", arr(0) \ "display")
                  case ("conceptURI", JString("")) => {
                    val system = (arr(0) \ "system").values.toString
                    val code = (arr(0) \ "code").values.toString

                    if (system == "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#")
                      ("conceptURI", JString("https://ncit.nci.nih.gov/ncitbrowser/pages/concept_details.jsf?dictionary=NCI+Thesaurus&code=" + code))
                    else
                      ("conceptURI", JString(system + code))
                  }
                  case other => other
                })
                // :+ ("caDSR", lookup.get)
                )

              case _ => JObject(enumValue.obj.mapConserve({
                case ("description", JString("")) => ("description", lookup.get \ "display")
                case other => other
              })
                // :+ ("caDSR", lookup.get)
              )
            }
          }
        }
        case other => other
      })))
      case other => other
    }))
  }
}
