/*
 * Copyright 2024 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.mock

import com.rawlabs.das.sdk.{DASExecuteResult, DASTable}
import com.rawlabs.protocol.das.{Qual, Row, SortKey}
import com.rawlabs.protocol.raw.Value
import com.typesafe.scalalogging.StrictLogging

class DASMockInMemoryTable(private val dasMockStorage: DASMockStorage) extends DASTable with StrictLogging {

  // Refer to the DASTable trait for more information on the getRelSize method
  override def getRelSize(quals: Seq[Qual], columns: Seq[String]): (Int, Int) = (dasMockStorage.size, 200)

  /**
   * The DASMockStorage wraps a TreeMap that stores the rows of the table. The tree map is already sorted by the key column.
   * Therefore, the table can provide the sorted results without any additional sorting. Can sort states that here.
   * If the TreeMap had a performant way to reverse the order, the table could also support reversed sorting by removing the !k.getIsReversed condition.
   */
  override def canSort(sortKeys: Seq[SortKey]): Seq[SortKey] = {
    sortKeys.filter(k => k.getName == "column1" && !k.getIsReversed)
  }

  // Refer to the DASTable trait for more information on the getPathKeys method
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

  /**
   * A SELECT statement is executed by calling the execute method.
   * Quals, colums, sortKeys, limit help to filter, project, sort and limit the results.
   * Implementing Quals, colums and limit is optional, but they can be useful to optimize the query execution.
   * Example: lets assume that this execute method calls an API that supports filtering, projection and limiting.
   * If the params are ignored, the API will return all the rows in the table and postgres will do the filtering, projection and limiting.
   * If the params are used and propagated to the backend of the API,
   * the results will be filtered, projected and limited, and the data volume transferred will be minimized.
   * If sortKeys are not empty, the execute method **has to** return the results sorted by the keys in the same order as the keys in the sortKeys param, postgres won't do it.
   * sortKeys are correlated to canSort method. Depending on what canSort returns, this param will either contain the keys or be empty.
   * @param quals - Qualifiers for filtering the rows
   * @param columns - Columns to project
   * @param maybeSortKeys - Sort keys. The sort keys are correlated to canSort method. Depending on what canSort returns, this param will either contain the keys or be empty.
   *                      If this param has values, then the execute method **has to** return the results sorted by the keys in the same order as the keys in the sortKeys param,
   *                      postgres will assume that the results are sorted.
   * @param maybeLimit - Limit the number of rows returned
   * @return
   */
  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      maybeSortKeys: Option[Seq[SortKey]],
      maybeLimit: Option[Long]
  ): DASExecuteResult = {
    logger.info(s"Executing query with quals: $quals, columns: $columns, sortKeys: $maybeSortKeys, limit: $maybeLimit")

    new DASExecuteResult {
      private val iterator = dasMockStorage.iterator
      private var currentIndex: Int = 1
      private var currentRow: Option[Row] = None
      private var nextCalled: Boolean = true
      private var hasNextValue: Boolean = false
      override def close(): Unit = {}

      // hasNext is called multiple times, before next is called
      override def hasNext: Boolean = {
        if (nextCalled) {
          currentRow = getNextRow(quals, iterator)
          hasNextValue = maybeLimit match {
            case Some(limit) => currentRow.isDefined && currentIndex <= Math.min(dasMockStorage.size, limit)
            case None => currentRow.isDefined && currentIndex <= dasMockStorage.size
          }
          nextCalled = false
        }
        hasNextValue
      }
      override def next(): Row = {
        currentIndex += 1
        nextCalled = true
        currentRow.get
      }
    }
  }

  private def getNextRow(quals: Seq[Qual], iterator: Iterator[Row]): Option[Row] = {
    while (iterator.hasNext) {
      val row = iterator.next()
      if (allPredicatesHold(quals, row)) {
        return Some(row)
      }
    }
    None
  }

  private def allPredicatesHold(quals: Seq[Qual], row: Row): Boolean = {
    quals.filter(_.hasSimpleQual).forall(predicateHolds(_, row))
  }

  private def predicateHolds(simpleQual: Qual, row: Row): Boolean = {
    val column = simpleQual.getFieldName
    val value = row.getDataMap.get(column).getInt.getV
    val operator = simpleQual.getSimpleQual.getOperator
    val targetValue = simpleQual.getSimpleQual.getValue.getInt.getV

    if (operator.hasGreaterThan) {
      value > targetValue
    } else if (operator.hasLessThan) {
      value < targetValue
    } else if (operator.hasEquals) {
      value == targetValue
    } else if (operator.hasNotEquals) {
      value != targetValue
    } else if (operator.hasGreaterThanOrEqual) {
      value >= targetValue
    } else if (operator.hasLessThanOrEqual) {
      value <= targetValue
    } else {
      throw new IllegalArgumentException(s"Unsupported operator: $operator")
    }
  }

  override def insert(row: Row): Row = dasMockStorage.add(row)

  override def bulkInsert(rows: Seq[Row]): Seq[Row] = rows.map(insert)

  override def update(rowId: Value, newValues: Row): Row = dasMockStorage.update(rowId.getInt.getV.toString, newValues)

  override def delete(rowId: Value): Unit = dasMockStorage.remove(rowId.getInt.getV.toString)
}
