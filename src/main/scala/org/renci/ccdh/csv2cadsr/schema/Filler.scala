package org.renci.ccdh.csv2cadsr.schema

import java.net.URI

import org.json4s.JsonAST.{JArray, JField, JObject, JString, JValue}
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
    scribe.info(s"Retrieving caDSR $caDSR.")

    val caDSRContent = Source.fromURL(s"https://fhir.hotecosystem.org/terminology/cadsr/ValueSet/$caDSR?_format=json")
    val caDSRJson = parse(caDSRContent.mkString(""))

    // Unfortunately, the FHIR HOT-E API doesn't work for this. So it's time to hit the caDSR API directly.
    // Step 1. Get the ValueDomain page and read its ID.
    val valueDomainXML = queryCaDSRAPI(s"query=ValueDomain&DataElement[@publicId=$caDSR][@latestVersionIndicator=Yes]&roleName=valueDomain")
    val valueDomainFields = XML.loadString(valueDomainXML) \\ "queryResponse" \ "class" \ "field"
    println(s"valueDomainFields: ${valueDomainFields}.")
    val valueDomainIds = valueDomainFields.filter(_.attribute("name") exists (_.text == "id")).map(_.text)

    val valuesetData = if (valueDomainIds.size != 1) Seq() else {
      // If we don't have exactly one, don't worry about it.
      val valueDomainId = valueDomainIds.head

      val valueDomainXML = queryCaDSRAPI(s"query=ValueDomainPermissibleValue&EnumeratedValueDomain[@id=$valueDomainId]&roleName=valueDomainPermissibleValueCollection")
      val
    }

    JObject(value.obj.mapConserve({
      case ("description", JString("")) => ("description", caDSRJson \ "description")
        /*
      case ("enumValues", enumValues: JObject) => JObject(enumValues.children.mapConserve({
        case other => other
      })*/
      case other => other
    }))
  }
}
