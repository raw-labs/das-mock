package com.rawlabs.das.mock

import com.rawlabs.protocol.das.Row

import scala.collection.mutable;

class DASMockStorage(
    val key: String,
    private val treeMap: mutable.TreeMap[String, Row] = new mutable.TreeMap[String, Row]()
) {
  def add(row: Row): Row = {
    treeMap.put(row.getDataMap.get(key).getInt.getV.toString, row)
    row
  }

  def update(rowId: String, row: Row): Row = {
    treeMap.put(rowId, row)
    row
  }

  def remove(rowId: String): Unit = treeMap.remove(rowId)

  def size: Int = treeMap.size

  def iterator: Iterator[Row] = treeMap.valuesIterator
}
