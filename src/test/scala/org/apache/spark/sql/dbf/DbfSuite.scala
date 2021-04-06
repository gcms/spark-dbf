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

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.CommonFileDataSourceSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession


class DbfSuite
  extends QueryTest
    with SharedSparkSession
    with CommonFileDataSourceSuite {

  override protected def dataSourceFormat = "dbf"

  val continents = testFile("continents.dbf")
  val books = testFile("books.dbf")
  val bdays = testFile("bdays.dbf")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.FILES_MAX_PARTITION_BYTES.key, 1024)
  }

  private def getResourceFilePath(name: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(name).toString
  }

  private def parseDate(dateStr: String): java.sql.Date = {
    new java.sql.Date(new SimpleDateFormat("dd/MM/yyyy").parse(dateStr).getTime)
  }

  test("correct values") {
    val df = spark.read.format("dbf").load(books)
    assert(df.head() == Row(1, "Dirk Luchte Comes Home", 5, 1985, "", "GGG&G Publishing",
      new java.math.BigDecimal("23.5000"), "Hard", parseDate("23/11/1993"), 1012, null))
  }

  test("counting single dbf file") {
    val df = spark.read.format("dbf").load(books)
    assert(df.count == 10)
  }

  test("couting using SQL") {
    val df = spark.read.format("dbf").load(books)
    df.createOrReplaceTempView("dbf_table")
    assert(spark.sql("select count(*) from dbf_table").collect().head === Row(10))
  }

  test("Read single field") {
    val df = spark.read.format("dbf").load(bdays)

    assert(df.select("LASTNAME").head() == Row("hendrix"))
  }

  test("Date field type") {
    val df = spark.read.format("dbf").load(bdays)
    val timestamp = df.head().get(1).asInstanceOf[Timestamp]

    assert("1942-11-27 10:15" == new SimpleDateFormat("YYYY-MM-dd hh:mm").format(timestamp))
  }


  test("convert formats") {
    withTempPath { dir =>
      val df = spark.read.format("dbf").load(books)
      df.write.parquet(dir.getCanonicalPath)
      assert(spark.read.parquet(dir.getCanonicalPath).count() === df.count)
    }
  }

  test("rearrange internal schema") {
    withTempPath { dir =>
      val df = spark.read.format("dbf").load(books)
      df.select("title", "publisher_").write.format("dbf").save(dir.getCanonicalPath)
    }
  }


  test("support of globbed paths") {
    val resourceDir = testFile(".")
    val e1 = spark.read.format("dbf").load(resourceDir + "../*/continents.dbf").collect()
    assert(e1.length == 7)

    val e2 = spark.read.format("dbf").load(resourceDir + "../../*/*/continents.dbf").collect()
    assert(e2.length == 7)
  }


  test("reading from invalid path throws exception") {

    // Directory given has no dbf files
    intercept[AnalysisException] {
      withTempPath(dir => spark.read.format("dbf").load(dir.getCanonicalPath))
    }

    intercept[AnalysisException] {
      spark.read.format("dbf").load("very/invalid/path/123.dbf")
    }

    // In case of globbed path that can't be matched to anything, another exception is thrown (and
    // exception message is helpful)
    intercept[AnalysisException] {
      spark.read.format("dbf").load("*/*/*/*/*/*/*/something.dbf")
    }
  }

  test("read DBC compressed") {
    val df = spark.read.format("dbf").load(testFile("test.dbc"))
    assert(df.count() == 26783)
  }

  test("test read write DBF") {
    withTempPath { dir =>
      val df = spark.read.format("dbf").load(testFile("test.dbc"))
      df.write.format("dbf").save(dir.getCanonicalPath + "/saida.dbf")

      val df2 = spark.read.format("dbf").load(dir.getCanonicalPath + "/saida.dbf")
      assert(df2.count() == 26783)
    }
  }
}
