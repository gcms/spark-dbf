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

import com.linuxense.javadbf.{DBFReader, DBFRow}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType

class DbfFileIterator(schema: StructType, reader: DBFReader)
  extends Iterator[InternalRow] {

  private var row: Option[DBFRow] = None

  override def hasNext: Boolean = {
    if (row.isEmpty) {
      row = Option(reader.nextRow())
    }

    row.isDefined
  }

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException("No more rows available")
    }
    val result = row.get
    row = None
    toInternalRow(result)
  }


  def toInternalRow(dbfRow: DBFRow): InternalRow = {
    val internalRow = new SpecificInternalRow(schema)
    schema.fields.indices.foreach(i => updateField(schema, dbfRow, internalRow, i))
    internalRow
  }

  def updateField(schema: StructType, dbfRow: DBFRow,
                  internalRow: InternalRow, ordinal: Int): Unit = {
    val field = schema.fields(ordinal)
    val value = DbfUtils.getRowValue(dbfRow, field)
    internalRow.update(ordinal, value)
  }

}
