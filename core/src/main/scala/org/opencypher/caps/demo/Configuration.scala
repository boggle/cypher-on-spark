/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.demo

object Configuration {

  abstract class ConfigOption[T](val name: String, val defaultValue: T)(convert: String => Option[T]) {
    def get(): T = Option(System.getProperty(name)).flatMap(convert).getOrElse(defaultValue)

    override def toString: String = {
      val filled = name + (name.length to 25).map(_ => " ").reduce(_ + _)
      s"$filled = ${get()}"
    }
  }

  object MasterAddress extends ConfigOption("cos.master", "local[*]")(Some(_))
  object Logging extends ConfigOption("cos.logging", "OFF")(Some(_))

  val conf = Seq(MasterAddress, Logging)

  def print(): Unit = {
    conf.foreach(println)
  }

}
