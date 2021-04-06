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

import com.linuxense.javadbf.DBFWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

class DbfOutputWriterFactory(conf: Configuration,
                             dbfOptions: DbfOptions)
  extends OutputWriterFactory with Serializable {
  override def getFileExtension(context: TaskAttemptContext): String = ".dbf"

  override def newInstance(path: String,
                           dataSchema: StructType,
                           context: TaskAttemptContext): OutputWriter = {
    new OutputWriter {
      private val writer = {
        val outputStream = CodecStreams.createOutputStream(context, new Path(path))
        val writer = new DBFWriter(outputStream, dbfOptions.getCharset)
        writeSchema(writer, dataSchema)
        writer
      }

      def writeSchema(writer: DBFWriter, dataSchema: StructType): Unit = {
        writer.setFields(dataSchema.fields.map(DbfUtils.toDbfField))
      }

      override def write(row: InternalRow): Unit = {
        val rec = dataSchema.fields.indices.map(i =>
          DbfUtils.toDbfValue(dataSchema.fields(i), i, row))
        writer.addRecord(rec.toArray)
      }

      override def close(): Unit = {
        writer.close()
      }
    }
  }
}
