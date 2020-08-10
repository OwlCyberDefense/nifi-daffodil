/*
 * Copyright 2020 Nteligen, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tresys.nifi.schema

import java.util.{ List => JList }

import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.serialization.record.`type`.{ArrayDataType, ChoiceDataType, RecordDataType}
import org.apache.nifi.serialization.record.{DataType, Record, RecordField, RecordSchema}

import scala.collection.JavaConverters

class RecordUtil

object RecordUtil {

  val PRODUCTION_MODE: Boolean = true

  def log(logger: ComponentLog, message: String, args: Array[Object]): Unit = {
    if (!PRODUCTION_MODE) {
      logger.error(message, args)
    }
  }

  def log(logger: ComponentLog, message: String): Unit = {
    log(logger, message, new Array[Object](0))
  }

  def printRecord(recordObject: Object, tabs: String): String = {
    recordObject match {
      case record: Record =>
        val builder: StringBuilder = new StringBuilder(tabs)
        builder.append("Record {\n")
        builder.append(
          JavaConverters.asScalaBuffer(record.getSchema.getFields)
            .filter(field => record.getValue(field.getFieldName) != null)
            .map( recordField => {
              tabs + "\t" + recordField.getFieldName + ": " +
                printRecord(record.getValue(recordField.getFieldName), tabs + "\t").substring(tabs.length + 1)
            }
            ).mkString(s",\n")
        )
        builder.append(s"\n$tabs}")
        builder.toString
      case arr: Array[Object] =>
        val builder: StringBuilder = new StringBuilder(tabs)
        builder.append("ARRAY [\n")
        builder.append(
          arr.map(child => tabs + printRecord(child, tabs + "\t").substring(tabs.length))
            .mkString(s",\n")
        )
        builder.append(s"\n$tabs]")
        builder.toString
      case _ => tabs + recordObject.toString
    }
  }

  def printRecordSchema(schema: RecordSchema): Unit = {
    println(printRecordSchemaHelper(schema, ""))
  }

  def getRecordPrefix(record: RecordSchema): String = s"Record {${record.getSchemaNamespace.orElse("")}}"

  def printRecordSchemaHelper(someObject: Object, tabs: String): String = {
    someObject match {
      case record: RecordSchema =>
        tabs + getRecordPrefix(record) + " {\n" +
          recordListAsString(record.getFields, tabs + "\t") + "\n" + tabs + "}"
      case recordType: RecordDataType => printRecordSchemaHelper(recordType.getChildSchema, tabs)
      case choiceDataType: ChoiceDataType =>
        tabs + "Choice [\n" +
          dataTypeListAsString(choiceDataType.getPossibleSubTypes, tabs + "\t") + "\n" + tabs + "]"
      case arrayDataType: ArrayDataType =>
        tabs + "ARRAY [\n" + printRecordSchemaHelper(arrayDataType.getElementType, tabs + "\t") +
          "\n" + tabs + "]"
      case dataType: DataType => tabs + dataType.toString
      case _ => throw new IllegalArgumentException(s"Unsupported type ${someObject.getClass}")
    }
  }

  private def recordFieldPrefix(field: RecordField): String = field match {
    case _: OptionalRecordField => "(Optional)" + field.getFieldName
    case _ => field.getFieldName
  }

  def recordListAsString(javaList: JList[RecordField], tabs: String): String = {
    JavaConverters.asScalaBuffer(javaList).map(
      recordField =>
        tabs + recordFieldPrefix(recordField) + ": " +
          printRecordSchemaHelper(recordField.getDataType, tabs).substring(tabs.length)
    ).mkString(",\n")
  }

  def dataTypeListAsString(javaList: JList[DataType], tabs: String): String = {
    JavaConverters.asScalaBuffer(javaList)
      .map(subType => tabs + printRecordSchemaHelper(subType, tabs).substring(tabs.length))
      .mkString(",\n")
  }
}
