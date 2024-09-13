package com.rawlabs.das.mock

import com.rawlabs.das.sdk.{DASExecuteResult, DASTable}
import com.rawlabs.protocol.das.{Qual, Row, SortKey}
import com.rawlabs.protocol.raw.Value
import com.typesafe.scalalogging.StrictLogging

class DASMockInMemoryTable(private val dasMockStorage: DASMockStorage) extends DASTable with StrictLogging {

  override def getRelSize(quals: Seq[Qual], columns: Seq[String]): (Int, Int) = (dasMockStorage.size, 200)

  override def canSort(sortKeys: Seq[SortKey]): Seq[SortKey] =
    Seq(SortKey.newBuilder().setName("column1").setIsReversed(false).build())

  override def getPathKeys: Seq[(Seq[String], Int)] = {
    Seq((Seq("column1"), 1))
  }

  override def uniqueColumn: String = dasMockStorage.key

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
      private val iterator = dasMockStorage.iterator
      override def close(): Unit = {}
      override def hasNext: Boolean = iterator.hasNext
      override def next(): Row = iterator.next()
    }
  }

  override def insert(row: Row): Row = dasMockStorage.add(row)

  override def bulkInsert(rows: Seq[Row]): Seq[Row] = rows.map(insert)

  override def update(rowId: Value, newValues: Row): Row = dasMockStorage.update(rowId.getInt.getV.toString, newValues)

  override def delete(rowId: Value): Unit = dasMockStorage.remove(rowId.getInt.getV.toString)
}
