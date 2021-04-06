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

import java.io.InputStream
import java.util.Date
import java.util.regex.Pattern

import com.github.gcms.dbc.DBCInputStream
import com.linuxense.javadbf._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils


private[sql] object DbfUtils extends Logging {
  def supportsDataType(dataType: DataType): Boolean = {
    dataType match {
      case _@(_: CharType | _: VarcharType) => true
      case _@(StringType | BooleanType | IntegerType
              | _: DecimalType | _: DataType | TimestampType) => true

      case _ => false
    }
  }


  def toDbfField(field: StructField): DBFField = {
    val rawType = CharVarcharUtils.getRawType(field.metadata)
    if (rawType.isDefined) {
      doGetDbfField(field, rawType.get)
    } else {
      doGetDbfField(field, field.dataType)
    }
  }

  private def doGetDbfField(field: StructField, dataType: DataType) = {
    dataType match {
      case dec: DecimalType =>
        new DBFField(field.name, DBFDataType.NUMERIC, dec.precision, dec.scale)

      case BooleanType => new DBFField(field.name, DBFDataType.LOGICAL)
      case IntegerType => new DBFField(field.name, DBFDataType.LONG)
      case LongType => new DBFField(field.name, DBFDataType.LONG)
      case DateType => new DBFField(field.name, DBFDataType.DATE)
      case TimestampType => new DBFField(field.name, DBFDataType.TIMESTAMP)

      case ch: CharType =>
        new DBFField(field.name, DBFDataType.CHARACTER, ch.length)

      // DBFWriter doesn't support VARCHAR when writing
      case vch: VarcharType =>
        new DBFField(field.name, DBFDataType.CHARACTER, vch.length)

      case StringType =>
        new DBFField(field.name, DBFDataType.CHARACTER, 100)

      case _ => throw new UnsupportedDbfTypeException("Invalid datatype " + field.dataType)
    }
  }

  def toDbfValue(field: StructField, ordinal: Int, row: InternalRow): AnyRef = {
    field.dataType match {
      case dec: DecimalType =>
        Option(row.getDecimal(ordinal, dec.precision, dec.scale)).map(_.toBigDecimal).orNull
      case _: CharType => row.getString(ordinal)
      case _: VarcharType => row.getString(ordinal)

      case BooleanType => new java.lang.Boolean(row.getBoolean(ordinal))
      case IntegerType => new java.lang.Integer(row.getInt(ordinal))
      case LongType => new java.lang.Long(row.getLong(ordinal))
      case DateType => new Date(row.getLong(ordinal))
      case TimestampType => new Date(row.getLong(ordinal))

      case _ => row.getString(ordinal)
    }
  }

  def getSqlSchemaFromDbfReader(dbfSchema: DBFReader): StructType = {
    val fields = (0 until dbfSchema.getFieldCount)
      .map(i => dbfSchema.getField(i))
      .map(toSqlTypeHelper)

    StructType(fields)
  }

  def toSqlTypeHelper(field: DBFField): StructField = {
    val dataType = field.getType match {
      case DBFDataType.LOGICAL => BooleanType
      case (DBFDataType.NUMERIC | DBFDataType.CURRENCY) =>
        DecimalType(field.getLength, field.getDecimalCount)
      case DBFDataType.FLOATING_POINT => DoubleType
      case DBFDataType.DATE => DateType
      case DBFDataType.TIMESTAMP => TimestampType
      case DBFDataType.LONG => if (field.getLength <= 10) {
        IntegerType
      } else {
        LongType
      }

      case DBFDataType.DOUBLE => DoubleType
      case DBFDataType.CHARACTER => CharType(field.getLength)
      case DBFDataType.VARCHAR => VarcharType(field.getLength)
      case DBFDataType.MEMO => StringType
      case _ => StringType
    }

    val metadata = new MetadataBuilder()
      .putLong("DbfDataType", field.getType.getCode)
      .putLong("DbfLength", field.getLength)
      .putLong("DbfDecimalCount", field.getDecimalCount)
      .build()
    StructField(field.getName, dataType, field.isNullable, metadata)
  }

  def getRowValue(row: DBFRow, field: StructField): Any = {
    getRowValue(row, field.name, field.dataType)
  }

  def getRowValue(row: DBFRow, fieldName: String, dataType: DataType): Any = {
    dataType match {
      case DecimalType() => Decimal(row.getBigDecimal(fieldName))
      case BooleanType => row.getBoolean(fieldName)
      case IntegerType => row.getInt(fieldName)
      case LongType => row.getLong(fieldName)
      case FloatType => row.getFloat(fieldName)
      case DoubleType => row.getDouble(fieldName)
      case DateType =>
        Option(row.getDate(fieldName))
          .map(d => new java.sql.Date(d.getTime))
          .map(DateTimeUtils.fromJavaDate).orNull
      case TimestampType =>
        Option(row.getDate(fieldName))
          .map(d => new java.sql.Timestamp(d.getTime))
          .map(DateTimeUtils.fromJavaTimestamp).orNull
      case _ => Option(row.getString(fieldName)).map(UTF8String.fromString).orNull

    }
  }

  def inferSchema(spark: SparkSession,
                  options: DbfOptions,
                  files: Seq[FileStatus]): Option[StructType] = {
    inferSchema(spark.sessionState.newHadoopConfWithOptions(options.parameters), options, files)
  }

  def inferSchema(conf: Configuration,
                  options: DbfOptions,
                  files: Seq[FileStatus]): Option[StructType] = {
    files
      .map(p => inferSchema(conf, options, p.getPath))
      .find(_.isDefined) // TODO: merge schemas instead of using first file
      .get

  }

  def inferSchema(conf: Configuration, options: DbfOptions, path: Path): Option[StructType] = {
    Utils.tryWithResource {
      openReader(conf, options, path)
    } { in =>
      try {
        Option(getSqlSchemaFromDbfReader(in))
      } catch {
        case e: DBFException =>
          if (options.ignoreCorruptFiles) {
            logWarning(s"Skipped the corrupted file: $path", e)
            None
          } else {
            throw new SparkException(s"Could not read file: $path", e)
          }
      }
    }
  }

  def openReader(conf: Configuration, options: DbfOptions, path: Path): DBFReader = {
    new DBFReader(openStream(conf, path), options.getCharset,
      options.showDeletedRows, options.supportExtendedCharacterFields)
  }

  def openStream(conf: Configuration, path: Path): InputStream = {
    wrapStream(path.getName, path.getFileSystem(conf).open(path))
  }

  private def wrapStream(fileName: String, inputStream: InputStream): InputStream = {
    if (isDbc(fileName)) {
      new DBCInputStream(inputStream)
    } else {
      inputStream
    }
  }

  private val DBC_PATTERN = Pattern.compile(".*\\.dbc", Pattern.CASE_INSENSITIVE)

  private def isDbc(path: String): Boolean = {
    DBC_PATTERN.matcher(path).matches()
  }
}

class UnsupportedDbfTypeException(msg: String) extends SparkException(msg)
