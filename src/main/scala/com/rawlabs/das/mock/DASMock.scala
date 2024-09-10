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
import com.rawlabs.protocol.das.{ColumnDefinition, FunctionDefinition, Qual, Row, SortKey, TableDefinition, TableId}
import com.rawlabs.protocol.raw.{IntType, StringType, Type, Value, ValueInt, ValueString}
import com.typesafe.scalalogging.StrictLogging

class DASMock extends DASSdk with StrictLogging {
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
        .build()
    )
  }

  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  override def getTable(name: String): Option[DASTable] = {
    name match {
      case "big" => Some(new DASMockTable(2000000000))
      case "small" => Some(new DASMockTable(100))
      case _ => None
    }
  }

  override def getFunction(name: String): Option[DASFunction] = None
}

class DASMockTable(maxRows: Int) extends DASTable with StrictLogging {

  override def getRelSize(quals: Seq[Qual], columns: Seq[String]): (Int, Int) = (maxRows, 200)

  override def canSort(sortKeys: Seq[SortKey]): Seq[SortKey] = Seq.empty

  override def getPathKeys: Seq[(Seq[String], Int)] = {
    Seq((Seq("column1"), 1))
  }

  override def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      maybeSortKeys: Option[Seq[SortKey]],
      maybeLimit: Option[Long],
      verbose: Boolean
  ): Seq[String] = Seq.empty

  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      maybeSortKeys: Option[Seq[SortKey]],
      maybeLimit: Option[Long]
  ): DASExecuteResult = {
    logger.info(s"Executing query with quals: $quals, columns: $columns, sortKeys: $maybeSortKeys, limit: $maybeLimit")

    new DASExecuteResult {
      private var currentIndex: Int = 1

      override def close(): Unit = {}

      override def hasNext: Boolean = {
        maybeLimit match {
          case Some(limit) => currentIndex <= Math.min(maxRows, limit)
          case None => currentIndex <= maxRows
        }
      }

      override def next(): Row = {
        if (!hasNext) throw new NoSuchElementException("No more elements")

        val row = Row
          .newBuilder()
          .putData("column1", Value.newBuilder().setInt(ValueInt.newBuilder().setV(currentIndex).build()).build())
          .putData(
            "column2",
            Value.newBuilder().setString(ValueString.newBuilder().setV(s"row_tmp_$currentIndex").build()).build()
          )
          .build()

        currentIndex += 1
        row
      }
    }
  }

}
