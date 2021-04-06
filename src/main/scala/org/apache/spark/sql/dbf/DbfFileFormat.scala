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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

class DbfFileFormat extends FileFormat with DataSourceRegister {

  override def shortName(): String = "dbf"

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = false

  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    val conf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val dbfOptions = new DbfOptions(options)

    DbfUtils.inferSchema(conf, dbfOptions, files)
  }


  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val dbfOptions = new DbfOptions(options)

    new DbfOutputWriterFactory(conf, dbfOptions)
  }

  override def buildReader(sparkSession: SparkSession,
                           dataSchema: StructType,
                           partitionSchema: StructType,
                           requiredSchema: StructType,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration):
  (PartitionedFile) => Iterator[InternalRow] = {

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))


    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val dbfOptions = new DbfOptions(options)


      val path = new Path(new URI(file.filePath))
      val schema = requiredSchema // vs dataSchema

      val reader = DbfUtils.openReader(conf, dbfOptions, path)

      // Ensure that the reader is closed even if the task fails or doesn't consume the entire
      // iterator of records.
      Option(TaskContext.get()).foreach { taskContext =>
        taskContext.addTaskCompletionListener[Unit] { _ =>
          reader.close()
        }
      }

      new DbfFileIterator(schema, reader)
    }
  }

  override def toString: String = "DBF"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[DbfFileFormat]

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }

}
