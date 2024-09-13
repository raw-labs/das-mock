package com.rawlabs.das.mock

import com.rawlabs.das.sdk.{DASExecuteResult, DASTable}
import com.rawlabs.protocol.das.{Qual, Row, SortKey}
import com.rawlabs.protocol.raw.{Value, ValueInt, ValueString}
import com.typesafe.scalalogging.StrictLogging

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
