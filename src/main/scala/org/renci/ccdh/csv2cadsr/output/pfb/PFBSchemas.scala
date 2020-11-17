package org.renci.ccdh.csv2cadsr.output.pfb

import java.util.Collections

import org.apache.avro.{Schema, SchemaBuilder}

/**
  * PFB schema documentation is at https://github.com/uc-cdis/pypfb#pfb-schema.
  *
  * It looks like a PFB file is an Avro file in the following format:
  * {
  *   "type" : "record",
  *   "name" : "Entity",
  *   "fields" : [ {
  *     "name" : "id",
  *     "type" : [ "null", "string" ],
  *     "default" : null
  *   }, {
  *     "name" : "name",
  *     "type" : "string"
  *   }, {
  *     "name" : "object",
  *     "type" : [ {
  *
  * }
  *
  * A PFB file contains a single entry with id=null, name=Metadata and object=a Metadata object that describes the
  * metadata of the columns in the file. This is followed by multiple entries with one or more of the object types
  * described in the original format.
  */

object PFBSchemas {
  val linkSchema: Schema = SchemaBuilder
    .record("Link")
    .fields()
    .name("multiplicity")
    .`type`()
    .enumeration("Multiplicity")
    .symbols("ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY")
    .noDefault()
    .requiredString("dst")
    .requiredString("name")
    .endRecord()

  val propertySchema: Schema = SchemaBuilder
    .record("Property")
    .fields()
    .requiredString("name")
    .requiredString("ontology_reference")
    .name("values")
    .`type`()
    .map()
    .values()
    .`type`("string")
    .noDefault()
    .endRecord()

  val nodeSchema: Schema = SchemaBuilder
    .record("Node")
    .fields()
    .requiredString("name")
    .requiredString("ontology_reference")
    .name("values")
    .`type`()
    .map()
    .values()
    .`type`("string")
    .noDefault()
    .name("links")
    .`type`()
    .array()
    .items(linkSchema)
    .noDefault()
    .name("properties")
    .`type`()
    .array()
    .items(propertySchema)
    .noDefault()
    .endRecord()

  val metadataSchema: Schema = SchemaBuilder
    .record("Metadata")
    .fields()
    .name("nodes")
    .`type`()
    .array()
    .items()
    .`type`(nodeSchema)
    .arrayDefault(java.util.List.of())
    .name("misc")
    .`type`()
    .map()
    .values()
    .`type`("string")
    .noDefault()
    .endRecord()

  def generatePFBForSchemas(schemas: Seq[Schema]): Schema = {
    // One of the schemas must be the metadataSchema.
    // Note that in https://github.com/uc-cdis/sc19-pfb-artifacts/tree/master/pfb-remake we
    // also have two additional schemas: one records the schema_version, the other records datetimes for create/update,
    // major/minor version and other metadata.
    var schemasBuilder = SchemaBuilder.unionOf().`type`(metadataSchema)
    schemas.foreach(schema => {
      schemasBuilder = schemasBuilder.and().`type`(schema)
    })

    val schemaBuilder = SchemaBuilder
      .record("Entity")
    val fieldsBuilder = schemaBuilder
      .fields()
      .nullableString("id", "null")
      .requiredString("name")
      .name("object")
      .`type`(schemasBuilder.endUnion())
      .noDefault()
      .name("relations")
      .`type`()
      .array()
      .items()
      .record("Relation")
      .fields()
      .requiredString("dst_id")
      .requiredString("dst_name")
      .endRecord()
      .arrayDefault(Collections.EMPTY_LIST)

    fieldsBuilder.endRecord()
  }
}
