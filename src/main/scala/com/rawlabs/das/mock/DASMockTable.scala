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
import com.rawlabs.protocol.raw.{Value, ValueInt, ValueString}
import com.typesafe.scalalogging.StrictLogging

class DASMockTable(maxRows: Int) extends DASTable with StrictLogging {

  override def getRelSize(quals: Seq[Qual], columns: Seq[String]): (Int, Int) = (maxRows, 200)

  /**
   * The DASMockStorage wraps a TreeMap that stores the rows of the table. The tree map is already sorted by the key column.
   * Therefore, the table can provide the sorted results without any additional sorting. Can sort states that here.
   * If the TreeMap had a performant way to reverse the order, the table could also support reversed sorting by removing the !k.getIsReversed condition.
   */
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
