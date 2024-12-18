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

import com.google.protobuf.ByteString
import com.rawlabs.das.sdk.{DASExecuteResult, DASTable}
import com.rawlabs.protocol.das.{Qual, Row, SortKey}
import com.rawlabs.protocol.raw.{
  Value,
  ValueBinary,
  ValueBool,
  ValueByte,
  ValueDate,
  ValueDecimal,
  ValueDouble,
  ValueFloat,
  ValueInt,
  ValueInterval,
  ValueList,
  ValueLong,
  ValueRecord,
  ValueRecordField,
  ValueShort,
  ValueString,
  ValueTime,
  ValueTimestamp
}

import java.time.{LocalDate, LocalDateTime, LocalTime}

class DASMockAllTypesTable(maxRows: Int) extends DASTable {

  override def getRelSize(quals: Seq[Qual], columns: Seq[String]): (Int, Int) = (maxRows, 200)

  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      maybeSortKeys: Option[Seq[SortKey]],
      maybeLimit: Option[Long]
  ): DASExecuteResult = {

    new DASExecuteResult {

      private val iterator = Range(1, maxRows + 1).iterator
      private val date0 = LocalDate.of(2021, 1, 1)
      private val time0 = LocalTime.of(15, 30, 0).plusNanos(123456789)
      private val timestamp0 = LocalDateTime.of(2021, 1, 1, 15, 30, 0).plusNanos(123456789)

      override def close(): Unit = {}

      override def hasNext: Boolean = iterator.hasNext

      override def next(): Row = {
        val i = iterator.next()
        val date = date0.plusDays(i)
        val time = time0.plusSeconds(i)
        val timestamp = timestamp0.plusSeconds(i).plusDays(i)

        val values: Map[String, Value] = Map(
          "byte_col" -> Value.newBuilder().setByte(ValueByte.newBuilder().setV(i).build()).build(),
          "short_col" -> Value.newBuilder().setShort(ValueShort.newBuilder().setV(i).build()).build(),
          "int_col" -> Value.newBuilder().setInt(ValueInt.newBuilder().setV(i).build()).build(),
          "long_col" -> Value.newBuilder().setLong(ValueLong.newBuilder().setV(i.toLong).build()).build(),
          "float_col" -> Value.newBuilder().setFloat(ValueFloat.newBuilder().setV(i * 0.33f)).build(),
          "double_col" -> Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(i * 0.55)).build(),
          "decimal_col" -> Value.newBuilder().setDecimal(ValueDecimal.newBuilder().setV(i.toString).build()).build(),
          "string_col" -> Value.newBuilder().setString(ValueString.newBuilder().setV(i.toString).build()).build(),
          "binary_col" -> Value
            .newBuilder()
            .setBinary(ValueBinary.newBuilder().setV(ByteString.copyFrom("Élu par cette crapule", "utf-8")).build())
            .build(),
          "bool_col" -> Value.newBuilder().setBool(ValueBool.newBuilder().setV(i % 2 == 0).build()).build(),
          "date_col" -> Value
            .newBuilder()
            .setDate(
              ValueDate
                .newBuilder()
                .setYear(date.getYear)
                .setMonth(date.getMonthValue)
                .setDay(date.getDayOfMonth)
                .build()
            )
            .build(),
          "time_col" -> Value
            .newBuilder()
            .setTime(
              ValueTime.newBuilder().setHour(time.getHour).setMinute(time.getMinute).setSecond(time.getSecond).build()
            )
            .build(),
          "timestamp_col" -> Value
            .newBuilder()
            .setTimestamp(
              ValueTimestamp
                .newBuilder()
                .setYear(timestamp.getYear)
                .setMonth(timestamp.getMonthValue)
                .setDay(timestamp.getDayOfMonth)
                .setHour(timestamp.getHour)
                .setMinute(timestamp.getMinute)
                .setSecond(timestamp.getSecond)
                .build()
            )
            .build(),
          "interval_col" -> Value
            .newBuilder()
            .setInterval(
              ValueInterval
                .newBuilder()
                .setYears(i)
                .setMonths(i)
                .setDays(i)
                .setHours(i)
                .setMinutes(i)
                .setSeconds(i)
                .build()
            )
            .build(),
          "any_col" -> {
            i % 3 match {
              case 0 => Value.newBuilder().setInt(ValueInt.newBuilder().setV(i).build()).build()
              case 1 => Value.newBuilder().setString(ValueString.newBuilder().setV("any string #" + i).build()).build()
              case 2 => Value.newBuilder().setBool(ValueBool.newBuilder().setV(i % 2 == 0).build()).build()
            }
          },
          "strings_col" -> {
            val items = (i to i + 10).map(j =>
              Value.newBuilder().setString(ValueString.newBuilder().setV("item #" + (j * i)).build()).build()
            )
            val listBuilder = ValueList.newBuilder()
            items.foreach(listBuilder.addValues)
            Value.newBuilder().setList(listBuilder.build()).build()
          },
          "timestamps_col" -> {
            val items = (i to i + 10).map(j => {
              val ts = timestamp.plusSeconds(j).plusDays(i)
              Value
                .newBuilder()
                .setTimestamp(
                  ValueTimestamp
                    .newBuilder()
                    .setYear(ts.getYear)
                    .setMonth(ts.getMonthValue)
                    .setDay(ts.getDayOfMonth)
                    .setHour(ts.getHour)
                    .setMinute(ts.getMinute)
                    .setSecond(ts.getSecond)
                    .build()
                )
                .build()
            })
            val listBuilder = ValueList.newBuilder()
            items.foreach(listBuilder.addValues)
            Value.newBuilder().setList(listBuilder.build()).build()
          },
          "record_col" -> {
            val record = Value
              .newBuilder()
              .setRecord(
                ValueRecord
                  .newBuilder()
                  .addFields(
                    ValueRecordField
                      .newBuilder()
                      .setName("intField")
                      .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(i).build()).build())
                  )
                  .addFields(
                    ValueRecordField
                      .newBuilder()
                      .setName("binaryField")
                      .setValue(
                        Value
                          .newBuilder()
                          .setBinary(
                            ValueBinary.newBuilder().setV(ByteString.copyFrom("Élu par cette crapule", "utf-8")).build()
                          )
                          .build()
                      )
                  )
                  .addFields(
                    ValueRecordField
                      .newBuilder()
                      .setName("timestampField")
                      .setValue(
                        Value
                          .newBuilder()
                          .setTimestamp(
                            ValueTimestamp
                              .newBuilder()
                              .setYear(timestamp.getYear)
                              .setMonth(timestamp.getMonthValue)
                              .setDay(timestamp.getDayOfMonth)
                              .setHour(timestamp.getHour)
                              .setMinute(timestamp.getMinute)
                              .setSecond(timestamp.getSecond)
                              .build()
                          )
                          .build()
                      )
                  )
                  .build()
              )
            record.build()
          },
          "str_record_col" -> {
            val record = Value
              .newBuilder()
              .setRecord(
                ValueRecord
                  .newBuilder()
                  .addFields(
                    ValueRecordField
                      .newBuilder()
                      .setName("str1")
                      .setValue(
                        Value.newBuilder().setString(ValueString.newBuilder().setV(s"str ${i + 1}").build()).build()
                      )
                  )
                  .addFields(
                    ValueRecordField
                      .newBuilder()
                      .setName("str2")
                      .setValue(
                        Value.newBuilder().setString(ValueString.newBuilder().setV(s"str ${i + 2}").build()).build()
                      )
                  )
                  .addFields(
                    ValueRecordField
                      .newBuilder()
                      .setName("str3")
                      .setValue(
                        Value.newBuilder().setString(ValueString.newBuilder().setV(s"str ${i + 3}").build()).build()
                      )
                  )
                  .build()
              )
            record.build()
          },
          "str_records_col" -> {
            val records = (i to i + 3).map(j => {
              val record = Value
                .newBuilder()
                .setRecord(
                  ValueRecord
                    .newBuilder()
                    .addFields(
                      ValueRecordField
                        .newBuilder()
                        .setName("str1")
                        .setValue(
                          Value.newBuilder().setString(ValueString.newBuilder().setV(s"str ${i + j}").build()).build()
                        )
                    )
                    .addFields(
                      ValueRecordField
                        .newBuilder()
                        .setName("str2")
                        .setValue(
                          Value.newBuilder().setString(ValueString.newBuilder().setV(s"str ${i + j + 1}").build()).build()
                        )
                    )
                    .addFields(
                      ValueRecordField
                        .newBuilder()
                        .setName("str3")
                        .setValue(
                          Value.newBuilder().setString(ValueString.newBuilder().setV(s"str ${i + j + 2}").build()).build()
                        )
                    )
                    .build()
                )
              record.build()
            })
            val listBuilder = ValueList.newBuilder()
            records.foreach(r => listBuilder.addValues(r))
            Value.newBuilder().setList(listBuilder.build()).build()
          },
          "records_col" -> {
            val records = (i to i + 3).map(j => {
              val ts = timestamp.plusSeconds(j).plusDays(i)
              Value
                .newBuilder()
                .setRecord(
                  ValueRecord
                    .newBuilder()
                    .addFields(
                      ValueRecordField
                        .newBuilder()
                        .setName("intField")
                        .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(j + i).build()).build())
                    )
                    .addFields(
                      ValueRecordField
                        .newBuilder()
                        .setName("binaryField")
                        .setValue(
                          Value
                            .newBuilder()
                            .setBinary(
                              ValueBinary
                                .newBuilder()
                                .setV(ByteString.copyFrom(s"Élu par cette crapule (${i + j})", "utf-8"))
                                .build()
                            )
                            .build()
                        )
                    )
                    .addFields(
                      ValueRecordField
                        .newBuilder()
                        .setName("timestampField")
                        .setValue(
                          Value
                            .newBuilder()
                            .setTimestamp(
                              ValueTimestamp
                                .newBuilder()
                                .setYear(ts.getYear)
                                .setMonth(ts.getMonthValue)
                                .setDay(ts.getDayOfMonth)
                                .setHour(ts.getHour)
                                .setMinute(ts.getMinute)
                                .setSecond(ts.getSecond)
                                .build()
                            )
                            .build()
                        )
                    )
                    .addFields(
                      ValueRecordField
                        .newBuilder()
                        .setName("timeField")
                        .setValue(
                          Value
                            .newBuilder()
                            .setTime(
                              ValueTime
                                .newBuilder()
                                .setHour(time.getHour)
                                .setMinute(time.getMinute)
                                .setSecond(time.getSecond)
                                .build()
                            )
                            .build()
                        )
                    )
                    .build()
                )
            })
            val listBuilder = ValueList.newBuilder()
            records.foreach(r => listBuilder.addValues(r))
            Value.newBuilder().setList(listBuilder.build()).build()
          }
        )
        val row = Row.newBuilder()
        for (col <- columns) row.putData(col, values(col))
        row.build()
      }

    }

  }
}