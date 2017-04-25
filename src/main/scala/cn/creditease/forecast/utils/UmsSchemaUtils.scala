package cn.creditease.forecast.utils

/**
  * Created by corlinchen on 2017/4/20.
  */
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import cn.creditease.forecast.utils.EdpCommon._
import org.json4s.ext.EnumNameSerializer

import scala.collection.mutable.ArrayBuffer

object UmsSchemaUtils extends UmsSchemaUtils

trait UmsSchemaUtils {
  json4sFormats = json4sFormats + new EnumNameSerializer(UmsProtocolType) + new EnumNameSerializer(UmsFieldType)

  /* ===================== ums utils ===================== */
  //def toUms1(json: String): Ums = json2caseClass[Ums](json)

  def toUms(json: String): Ums = {
    val jsonObj: JSONObject = JSON.parseObject(json)
    val protocol = jsonObj.getJSONObject("protocol").getString("type")
    val schema = jsonObj.getJSONObject("schema")
    var context:Option[String] = None
    if(jsonObj.containsKey("context")){
      context=Some(jsonObj.getString("context"))
    }

    val umsSchema = toUmsSchemaFromJsonObject(schema)

    val payloadArr: Option[Seq[UmsTuple]] = if (jsonObj.containsKey("payload") && jsonObj.getJSONArray("payload").size() > 0) {
      val payloadJsonArr = jsonObj.getJSONArray("payload")
      val payloadSize = payloadJsonArr.size()
      val tmpPayload: Array[UmsTuple] = new Array[UmsTuple](payloadSize)
      for (i <- 0 until payloadSize) {
        val tuple: JSONObject = payloadJsonArr.get(i).asInstanceOf[JSONObject]
        val tupleJsonArr: JSONArray = tuple.getJSONArray("tuple")
        val tupleSeq = new ArrayBuffer[String]()
        for (j <- 0 until tupleJsonArr.size()) {
          val ele = tupleJsonArr.get(j)
          val tuple = if (ele == null) null else ele.toString
          tupleSeq += tuple
        }

        val tupleArr = UmsTuple(tupleSeq)
        tmpPayload(i) = tupleArr
      }
      Some(tmpPayload)
    } else {
      None
    }

    Ums(UmsProtocol(UmsProtocolType.umsProtocolType(protocol)),
      umsSchema,
      payloadArr,
      context)
  }


  def toJsonCompact(ums: Ums): String = jsonCompact(caseClass2json[Ums](ums))
  def toFastJsonCompact(ums: Ums): String = {
    toJson(ums,false)
  }


  def toJsonPretty(ums: Ums): String = jsonPretty(caseClass2json[Ums](ums))
  def toFastJsonPretty(ums: Ums): String = {
    toJson(ums,true)
  }

  def toJson(ums: Ums,bool: Boolean): String = {

    val protocol:UmsProtocol = ums.protocol
    val umsSchema: UmsSchema = ums.schema

    val umsMap = new java.util.HashMap[String,Object]

    // protocol
    val protocolTypeMap = new java.util.HashMap[String,Object]()
    protocolTypeMap.put("type",protocol.`type`.toString)
    umsMap.put("protocol",protocolTypeMap)

    // schema:Namespace
    val schemaNamespaceMap = new java.util.HashMap[String,Object]()
    schemaNamespaceMap.put("namespace",umsSchema.namespace)

    // schema:Field
    val listField = umsSchema.fields_get.toList
    val schemaFieldList = new java.util.ArrayList[Object]()
    for (i <- listField.indices) {
      val schemaFieldMap = new java.util.HashMap[String,Object]
      schemaFieldMap.put("name",listField(i).name.toString)
      schemaFieldMap.put("type",listField(i).`type`.toString)
      schemaFieldMap.put("nullable",listField(i).nullable.mkString)
      schemaFieldList.add(schemaFieldMap)
    }
    if (listField.nonEmpty) schemaNamespaceMap.put("fields",schemaFieldList)

    umsMap.put("schema",schemaNamespaceMap)

    // payload
    //    val listPayload = ums.payload_get.toList
    val payloadList = new java.util.ArrayList[Object]

    val umsTupleList: List[UmsTuple] = ums.payload_get.toList


    for (i <- umsTupleList.indices) {
      val tupleMap = new java.util.HashMap[String,Object]
      val umsTuple:UmsTuple = umsTupleList(i)
      tupleMap.put("tuple",umsTuple.tuple.toArray)
      payloadList.add(tupleMap)
    }
    if(umsTupleList.nonEmpty) umsMap.put("payload",payloadList)

    JSON.toJSONString(umsMap,bool).replace("\"false\"","false").replace("\"true\"","true")
  }

  /* ===================== ums schema utils ===================== */
  //def toUmsSchema(json: String): UmsSchema = json2caseClass[UmsSchema](json)
  def parseUmsSchema(json: String): UmsSchema = {
    val schema: JSONObject = JSON.parseObject(json)
    toUmsSchemaFromJsonObject(schema)
  }

  def toUmsSchema(json: String): UmsSchema = {
    val schema: JSONObject = JSON.parseObject(json)
    toUmsSchemaFromJsonObject(schema.getJSONObject("schema"))
  }

  private def toUmsSchemaFromJsonObject(schema: JSONObject): UmsSchema = {
    val namespace = schema.getString("namespace").toLowerCase
    val fields = if (schema.containsKey("fields") && schema.getJSONArray("fields").size() > 0) {
      val fieldsArr: JSONArray = schema.getJSONArray("fields")
      val tmpFields: Array[UmsField] = new Array[UmsField](fieldsArr.size())
      for (i <- 0 until fieldsArr.size()) {
        val oneField: JSONObject = fieldsArr.get(i).asInstanceOf[JSONObject]
        val `type` = UmsFieldType.umsFieldType(oneField.getString("type"))
        val name = oneField.getString("name").toLowerCase
        val nullable: Boolean = if (oneField.containsKey("nullable")) oneField.getBoolean("nullable") else false
        tmpFields(i) = UmsField(name, `type`, Some(nullable))
      }
      Some(tmpFields.toList)
    } else {
      None
    }
    UmsSchema(namespace, fields)
  }

  def toJsonSchemaCompact(schema: UmsSchema): String = jsonCompact(caseClass2json[UmsSchema](schema))
  def toFastJsonSchemaCompact(schema: UmsSchema,isFull:Boolean): String = {
    toFastJsonSchema(schema,false,isFull)
  }

  def toJsonSchemaPretty(schema: UmsSchema): String = jsonPretty(caseClass2json[UmsSchema](schema))
  def toFastJsonSchemaPretty(schema: UmsSchema,isFull:Boolean): String = {
    toFastJsonSchema(schema,true,isFull)
  }

  def toFastJsonSchema(schema:UmsSchema,bool: Boolean,isFull:Boolean):String = {
    val umsSchema: UmsSchema = schema

    // schema:Namespace
    val schemaNamespaceMap = new java.util.HashMap[String,Object]()
    schemaNamespaceMap.put("namespace",umsSchema.namespace)

    // schema:Field
    val listField = umsSchema.fields_get.toList
    val schemaFieldList = new java.util.ArrayList[Object]()
    for (i <- listField.indices) {
      val schemaFieldMap = new java.util.HashMap[String,Object]
      schemaFieldMap.put("name",listField(i).name.toString)
      schemaFieldMap.put("type",listField(i).`type`.toString)
      if(isFull) schemaFieldMap.put("nullable",listField(i).nullable.mkString)
      schemaFieldList.add(schemaFieldMap)
    }
    if (listField.nonEmpty) schemaNamespaceMap.put("fields",schemaFieldList)

    JSON.toJSONString(schemaNamespaceMap,bool).replace("\"false\"","false").replace("\"true\"","true")
  }
}
