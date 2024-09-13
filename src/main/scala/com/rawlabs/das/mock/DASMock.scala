/**
 * Copyright 2024 RAW Labs S.A.
 * All rights reserved.
 *
 * This source code is the property of RAW Labs S.A. It contains
 * proprietary and confidential information that is protected by applicable
 * intellectual property and other laws. Unauthorized use, reproduction,
 * or distribution of this code, or any portion of it, may result in severe
 * civil and criminal penalties and will be prosecuted to the maximum
 * extent possible under the law.
 */

package com.rawlabs.das.mock

import com.rawlabs.das.sdk._
import com.rawlabs.protocol.das.{ColumnDefinition, FunctionDefinition, TableDefinition, TableId}
import com.rawlabs.protocol.raw.{IntType, StringType, Type}
import com.typesafe.scalalogging.StrictLogging

class DASMock extends DASSdk with StrictLogging {

  private val dasMockStorage = new DASMockStorage("column1")

  def this(options: Map[String, String]) = {
    this()
    options.keys.foreach(key => logger.info(s"Option: $key = ${options(key)}"))
  }

  override def tableDefinitions: Seq[TableDefinition] = {
    Seq(
      TableDefinition
        .newBuilder()
        .setTableId(TableId.newBuilder().setName("big"))
        .setDescription("A mock table")
        .addColumns(
          ColumnDefinition
            .newBuilder()
            .setName("column1")
            .setDescription("The first column - int")
            .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
            .build()
        )
        .addColumns(
          ColumnDefinition
            .newBuilder()
            .setName("column2")
            .setDescription("The second column - string")
            .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
            .build()
        )
        .setStartupCost(1000)
        .build(),
      TableDefinition
        .newBuilder()
        .setTableId(TableId.newBuilder().setName("small"))
        .setDescription("A mock table")
        .addColumns(
          ColumnDefinition
            .newBuilder()
            .setName("column1")
            .setDescription("The first column - int")
            .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
            .build()
        )
        .addColumns(
          ColumnDefinition
            .newBuilder()
            .setName("column2")
            .setDescription("The second column - string")
            .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
            .build()
        )
        .setStartupCost(1000)
        .build(),
      TableDefinition
        .newBuilder()
        .setTableId(TableId.newBuilder().setName("in_memory"))
        .setDescription("A mock in memory table")
        .addColumns(
          ColumnDefinition
            .newBuilder()
            .setName("column1")
            .setDescription("The first column - int")
            .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
            .build()
        )
        .addColumns(
          ColumnDefinition
            .newBuilder()
            .setName("column2")
            .setDescription("The second column - string")
            .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
            .build()
        )
        .setStartupCost(2000)
        .build()
    )
  }

  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  override def getTable(name: String): Option[DASTable] = {
    name match {
      case "big" => Some(new DASMockTable(2000000000))
      case "small" => Some(new DASMockTable(100))
      case "in_memory" => Some(new DASMockInMemoryTable(dasMockStorage))
      case _ => None
    }
  }

  override def getFunction(name: String): Option[DASFunction] = None
}
