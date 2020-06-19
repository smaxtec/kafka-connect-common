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

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths
import java.util.{Collections, UUID}

import com.datamountaineer.streamreactor.connect.converters.MsgKey
import com.sksamuel.avro4s.{RecordFormat, SchemaFor}

import io.confluent.connect.avro.AvroData
import org.apache.avro.Schema
import org.apache.kafka.connect.data.Struct
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JsonConverterWithAvroSchemaTest extends AnyWordSpec with Matchers {
  val topic = "the_real_topic"
  val sourceTopic = "source_topic"
  val avroData = new AvroData(4)
  private val folder = new File(UUID.randomUUID().toString)
  folder.mkdir()


  private def initializeConverter(converter: JsonConverterWithAvroSchema, schema: Schema) = {
    def writeSchema(schema: Schema): File = {
      val schemaFile = Paths.get(folder.getName, UUID.randomUUID().toString)
      val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
      bw.write(schema.toString)
      bw.close()

      schemaFile.toFile
    }

    converter.initialize(Map(
      converter.SCHEMA_CONFIG -> s"${writeSchema(schema)}"
    ))

  }

  "JsonConverter" should {

    "handle a avro schema" in {
      val converter = new JsonConverterWithAvroSchema
      val carList = List[AvroCar](new AvroCar("n", Option("m"), 3, 1, 2.2))
      val cars = CarTransporter("test", Option(new AvroCar("Truck", Option("m"), 3, 1, 2.2)), carList, Option("manuf."), Option(22), 3, 2.1)
      val avro = RecordFormat[CarTransporter].to(cars)
      print(avro.getSchema)
      initializeConverter(converter, avro.getSchema)
      //
      val json =
        """
          |{
          |  "name": "transporter1",  "cars" : [{"name": "CAR1", "manufacturer": "man1", "model": 3, "bhp": 1, "price": 3}],
          |  "manufacturer": null, "bhp": 3, "price": 3.1
          |}
        """.stripMargin
      val record = converter.convert(topic, sourceTopic, "100", json.getBytes)
      record.value().asInstanceOf[Struct].getString("name") shouldBe "transporter1"
      record.value().asInstanceOf[Struct].getString("manufacturer") shouldBe null
      record.value().asInstanceOf[Struct].getStruct("truck") shouldBe null
      record.value().asInstanceOf[Struct].getInt64("model") shouldBe null
      record.value().asInstanceOf[Struct].getArray("cars").get(0).asInstanceOf[Struct].getString("name") shouldBe "CAR1"

      val json2 =
        """
          |{
          |  "name": "transporter1", "truck": {"name": "truckcar", "manufacturer": "man1", "model": 3, "bhp": 1, "price": 3}, "cars" : [{"name": "CAR1", "model": 3, "bhp": 1, "price": 3}],
          |  "manufacturer": "man2", "bhp": 3, "price": 3, "model": 31
          |}
        """.stripMargin
      val record2 = converter.convert(topic, sourceTopic, "100", json2.getBytes)
      record2.value().asInstanceOf[Struct].getString("name") shouldBe "transporter1"
      record2.value().asInstanceOf[Struct].getString("manufacturer") shouldBe "man2"
      record2.value().asInstanceOf[Struct].getStruct("truck").getString("name") shouldBe "truckcar"
      record2.value().asInstanceOf[Struct].getInt64("model") shouldBe 31
      record2.value().asInstanceOf[Struct].getArray("cars").get(0).asInstanceOf[Struct].getString("name") shouldBe "CAR1"
    }
  }
}


case class AvroCar(name: String,
               manufacturer: Option[String],
               model: Long,
               bhp: Int,
               price: Double)


case class CarTransporter(name: String,
                          truck: Option[AvroCar],
                          cars: List[AvroCar],
                          manufacturer: Option[String],
                          model: Option[Long],
                          bhp: Long,
                          price: Double)