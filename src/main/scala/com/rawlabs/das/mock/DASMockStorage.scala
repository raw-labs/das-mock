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

import com.rawlabs.protocol.das.Row

import scala.collection.mutable;

// all methods are synchronized to ensure thread safety
// alternatively one could use immutable data structures
class DASMockStorage(
    val key: String
) {
  private val treeMap: mutable.TreeMap[String, Row] = new mutable.TreeMap[String, Row]()
  private val treeMapLock = new Object

  def add(row: Row): Row = treeMapLock.synchronized {
    treeMap.put(row.getDataMap.get(key).getInt.getV.toString, row)
    row
  }

  def update(rowId: String, row: Row): Row = treeMapLock.synchronized {
    treeMap.put(rowId, row)
    row
  }

  def remove(rowId: String): Unit = treeMapLock.synchronized(treeMap.remove(rowId))

  def size: Int = treeMapLock.synchronized(treeMap.size)

  // clone the values to avoid concurrent modification
  def iterator: Iterator[Row] = treeMapLock.synchronized(treeMap.clone().valuesIterator)
}
