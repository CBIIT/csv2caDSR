package org.renci.ccdh.csv2cadsr.schema

import org.json4s.JsonAST.{JArray, JObject, JString, JValue}
import org.json4s.native.JsonMethods._

import scala.io.Source

/**
 * The JSONMappingFiller fills in a JSON mapping file. At the moment, it does this solely
 * by looking up information in the caDSR via the [caDSR-on-FHIR service](https://github.com/HOT-Ecosystem/cadsr-on-fhir/).
 * In the future, however, we might incorporate other data sources into this process as well.
 */
object JSONMappingFiller {
  /**
    * Given an object with a list of properties as fields, fill each property based on its caDSR information
    * (assuming that it hasn't been filled in yet).
    *
    * @param props Really a JObject, where each field is a property.
    * @return The original JObject passed in, but with values filled in.
    */
  def fillProperties(props: JValue): JValue = props.mapField({
    case (propName, property: JObject) => (propName, fillProperty(propName, property))
    case other => other
  })

  /**
    * Fill in a single property based on the available information (as long as it hasn't been filled in already).
    *
    * @param propName The name of the property to be filled in.
    * @param propValue The value of the property to be filled in.
    * @return The filled-in property (assuming that it isn't filled in already).
    */
  def fillProperty(propName: String, propValue: JObject): JObject = {
    val element = (propValue \ "caDSR")
    element match {
      case JString(caDSR) => fillPropertyFromCaDSR(propName, propValue, caDSR)
      case _ => propValue
    }
  }

  /**
    * Fill in this property with information from the [caDSR-on-FHIR service](https://github.com/HOT-Ecosystem/cadsr-on-fhir/).
    * We fill in both top-level information (e.g. description, permissibleValues) as well as the individual values
    * listed in enumValues (e.g. description, conceptURI).
    *
    * @param propName The name of this property.
    * @param value The property, as described in a JObject.
    * @param cdeId The caDSR CDE ID.
    * @return The filled in property (or the original property, if it was already filled in).
    */
  def fillPropertyFromCaDSR(propName: String, value: JObject, cdeId: String): JObject = {
    if (cdeId.isEmpty) return value

    scribe.info(s"Retrieving caDSR $cdeId.")

    // Retrieve information about this valueset from the caDSR-on-FHIR service.
    val caDSRContent = Source.fromURL(s"https://fhir.hotecosystem.org/terminology/cadsr/ValueSet/$cdeId?_format=json")
    val caDSRJson = parse(caDSRContent.mkString(""))
    val caDSRPermissibleValues = (caDSRJson \ "expansion" \ "contains").toOption match {
      case None => JArray(List())
      case Some(arr: JArray) => arr
      case _ => throw new Error(s".expansion.contains in JSON request is not an array as expected: ${caDSRJson}")
    }
    val caDSRPVCodeMap = caDSRPermissibleValues.arr.map({
      case codeEntry: JObject => (codeEntry.values.getOrElse("code", "(none)"), codeEntry)
    }).toMap

    // Create a new JObject, filling in any blank or empty values.
    JObject(value.obj.mapConserve({
      // Fill in an empty description.
      case ("description", JString("")) => ("description", caDSRJson \ "description")
      // Fill in an empty set of permissibleValues.
      case ("permissibleValues", JArray(List())) => ("permissibleValues", JArray(caDSRPVCodeMap.keys.map(key => JString(key.toString)).toList))
      // enumValues itself contains enumerated values that need to be mapped.
      case ("enumValues", enumValues: JArray) => ("enumValues", JArray(enumValues.arr.mapConserve({
        case enumValue: JObject => {
          // To map enumValues, the user must provide a caDSRValue.
          val caDSRValue: String = enumValue.values.getOrElse("caDSRValue", "").toString
          // Is there a mapping for the caDSR value?
          if (!caDSRPVCodeMap.contains(caDSRValue)) {
            // If not, there's nothing we can do for this enumValue.
            scribe.info(s"Could not map enumValue ${enumValue \ "value"}: not found in list of permissible values")
            enumValue
          } else {
            // Found a match!
            val lookup = caDSRPVCodeMap.get(caDSRValue)
            val coding = (lookup.get.findField(_._1 == "coding").map(_._2))
            coding match {
              case Some(arr: JArray) => JObject(enumValue.obj.mapConserve({
                  case ("description", JString("")) => ("description", arr(0) \ "display")
                  case ("conceptURI", JString("")) => {
                    // This is provided to us in two pieces:
                    //  - system (e.g. "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#")
                    //  - code (e.g. C41260)
                    // I think we can usually concatenate them together into an ID.
                    val system = (arr(0) \ "system").values.toString
                    val code = (arr(0) \ "code").values.toString

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
