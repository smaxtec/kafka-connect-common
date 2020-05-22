/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.converters.source

import java.io.File
import java.nio.charset.Charset
import java.util
import java.util.Collections

import scala.collection.JavaConversions._

import com.datamountaineer.streamreactor.connect.converters.MsgKey
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.source.SourceRecord
import org.apache.avro.{Schema => AvroSchema}

import io.confluent.connect.avro.AvroData

class JsonConverterWithAvroSchema extends Converter {
  val SCHEMA_CONFIG = "connect.source.converter.jsonconverterwithavroschema.schema"
  private val avroData = new AvroData(1)
  private var schema: Schema = null

  override def convert(kafkaTopic: String,
                       sourceTopic: String,
                       messageId: String,
                       bytes: Array[Byte],
                       keys:Seq[String] = Seq.empty,
                       keyDelimiter:String = "."): SourceRecord = {
    require(bytes != null, s"Invalid $bytes parameter")
    val json = new String(bytes, Charset.defaultCharset)
    val value = JsonConverterWithAvroSchema.convert(sourceTopic, json, schema)
    value match {
      case s:Struct if keys.nonEmpty =>
        val keysValue = keys.flatMap { key =>
          Option(KeyExtractor.extract(s, key.split('.').toVector)).map(_.toString)
        }.mkString(keyDelimiter)

        new SourceRecord(Collections.singletonMap(Converter.TopicKey, sourceTopic),
          null,
          kafkaTopic,
          Schema.STRING_SCHEMA,
          keysValue,
          schema, 
          value)
      case _=>
        new SourceRecord(Collections.singletonMap(Converter.TopicKey, sourceTopic),
          null,
          kafkaTopic,
          MsgKey.schema,
          MsgKey.getStruct(sourceTopic, messageId),
          schema,
          value)
    }

  }

  override def initialize(config: Map[String, String]): Unit = {
    schema = avroData.toConnectSchema(new AvroSchema.Parser().parse(new File(config.get(SCHEMA_CONFIG).get)))
  }
}

object JsonConverterWithAvroSchema {

  import org.json4s._
  import org.json4s.native.JsonMethods._

  def convert(name: String, str: String, schema: Schema): AnyRef = convert(name, parse(str), schema)

  def convert(name: String, value: JValue, schema: Schema ): AnyRef = {
    implicit val formats = DefaultFormats

    schema.`type`() match{
      case Schema.Type.STRUCT =>
        val struct = new Struct(schema)
        schema.fields().toList.foreach{ field =>
          val fieldSchema = field.schema()
          val jValue = value.asInstanceOf[JObject] \ field.name()
          if (jValue == JNothing && fieldSchema.isOptional()) {
            struct.put(field.name(), null)
          }
          else {
            val v = convert(field.name(), jValue, field.schema())
            struct.put(field.name(), v)
          }
        }
        struct
      case Schema.Type.ARRAY =>
        val jsonData = value.asInstanceOf[JArray]
        val values = new util.ArrayList[AnyRef]()
        jsonData.arr.foreach { v => values.add(convert(name, v, schema.valueSchema())) }
        return values
      case Schema.Type.INT8 =>
        value.extract[Integer]
      case Schema.Type.INT16 =>
        value.extract[Integer]
      case Schema.Type.INT32 =>
        value.extract[Integer]
      case Schema.Type.INT64 =>
        new java.lang.Long(value.extract[Long])
      case Schema.Type.FLOAT32 =>
        new java.lang.Float(value.extract[Float])
      case Schema.Type.FLOAT64 =>
        new java.lang.Double(value.extract[Double])
      case Schema.Type.BOOLEAN =>
        new java.lang.Boolean(value.extract[Boolean])
      case Schema.Type.STRING =>
        value.extract[String]
      case _ =>
        throw new Exception ("Type not supported yet for field " + name + " with schema " + schema + " and val " + value)
    }



  }
}
