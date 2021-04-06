/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.dbf

import java.nio.charset.Charset

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf

class DbfOptions(@transient val parameters: CaseInsensitiveMap[String]) extends Serializable {

  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters))
  }

  val charsetName: String = parameters.getOrElse("charset", Charset.defaultCharset().name())

  val showDeletedRows: Boolean = parameters.get("showDeletedRows").exists(_.toBoolean)

  val supportExtendedCharacterFields: Boolean =
    parameters.get("supportExtendedCharacterFields").forall(_.toBoolean);

  def getCharset: Charset = {
    Charset.forName(charsetName)
  }

  def ignoreCorruptFiles: Boolean = SQLConf.get.ignoreCorruptFiles

}
