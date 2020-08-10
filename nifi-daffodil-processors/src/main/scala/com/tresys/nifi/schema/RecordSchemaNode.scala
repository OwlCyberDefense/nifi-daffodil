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

import java.util.{ Locale => JLocale }

import org.apache.daffodil.dsom.walker._
import org.apache.nifi.serialization.record.RecordFieldType

/**
 * A temporary Node representing a single item in the DSOM tree.  These are generated
 * as various nodes are encountered during the walk defined in RecordWalker.
 * @param dsomElement the original DSOM Term or ModelGroup from which attributes will be obtained
 */
class RecordSchemaNode(dsomElement: TermView) {
  // If this Node represents a simple type, this is the exact simple type it is in terms of a Record
  final val simpleType: Option[RecordFieldType] = dsomElement match {
    case element: ElementDeclView =>
      if (element.isSimpleType) {
        val recordFieldType: RecordFieldType = element.simpleType.primType match {
          /**
           * While I suspect all the "regular" primitives and the String type work,
           * all that has been tested so far are UnsignedInts, which get turned into Longs,
           * and Strings.  The date/time-based types may or may not be compatible with NIFI's notion
           * of date and time
           */
          case _: DateTimeView => RecordFieldType.TIMESTAMP
          case _: DateView => RecordFieldType.DATE
          case _: TimeView => RecordFieldType.TIME
          case _: BooleanView => RecordFieldType.BOOLEAN
          case _: ByteView => RecordFieldType.BYTE
          case _: UnsignedByteView | _: ShortView => RecordFieldType.SHORT
          case _: UnsignedShortView | _: IntView => RecordFieldType.INT
          case _: UnsignedIntView | _: LongView => RecordFieldType.LONG
          case _: UnsignedLongView => RecordFieldType.BIGINT
          case _: FloatView => RecordFieldType.FLOAT
          case _: DecimalView | _: DoubleView => RecordFieldType.DOUBLE
          case _: StringView | _: HexBinaryView | _: AnyURIView => RecordFieldType.STRING
          case _ => throw new IllegalArgumentException(s"unsupported type for element $element")
        }
        Some(recordFieldType)
      } else None
    case _ => None
  }

  /* This is going to become the name of a RecordField.
   * Choices should be the only SchemaNodes that don't have names after postProcessing,
   * except for a few wrapper Records that are necessary in some cases
   */
  final val name: Option[String] = dsomElement match {
    case elementBase: ElementBaseView => Some(elementBase.name)
    case _ => None
  }

  final val isSimple: Boolean = name.isDefined && simpleType.isDefined

  /**
   * Represents if this SchemaNode should be treated as an OptionalRecordField
   */
  final val isOptional: Boolean = dsomElement match {
    case element: ElementBaseView => element.isOptional
    case _ => false
  }

  final val recordType: RecordFieldType = dsomElement match {
    case _: ChoiceView => RecordFieldType.CHOICE
    // Sequences aren't array types; they are treated like Records
    case _: SequenceView => RecordFieldType.RECORD
    /**
     * If an element is a simple type, that simple type just becomes its record type.
     */
    case element: ElementBaseView =>
      if (element.isArray) {
        RecordFieldType.ARRAY
      } else if (isSimple) {
        simpleType.get
      } else RecordFieldType.RECORD
    // probably isn't the *best* solution, but this is a surefire way to ensure nothing other than the above
    // types are passed to the constructor of this class.
    case _ => throw new IllegalArgumentException(s"Unsupported type ${dsomElement.getClass}")
  }

  final val namespace: String = dsomElement.namespaces.uri

  /**
   * These represent any type of embedded DFDL element that is a sub-element of this SchemaNode
   * Once postProcessing is done, this should mostly only be items of an Array, the options for
   * a Choice, or the list of fields for a Record
   */
  var children: List[RecordSchemaNode] = List()

  def addChild(newChild: RecordSchemaNode): Unit = {
    children = children :+ newChild
  }

  /**
   * This is all used to get nice, printable representations of the SchemaNode tree before
   * it is all converted to Records. Generally just used for debugging purposes;
   * namely, to see if the RecordSchema matches the original SchemaNode tree
   */
  private final val nameAttr: String =
    if (name.isDefined && simpleType.isDefined) {
      s" ${name.get}='${simpleType.get}'"
    } else if (name.isDefined) s" name='${name.get}'" else ""
  private final val optionalAttr: String = if (isOptional) " optional" else ""
  private final val startingStr
    = s"<${recordType.toString.toLowerCase(JLocale.ROOT)}$nameAttr$optionalAttr {$namespace}>"
  private final val endingStr = s"</${recordType.toString.toLowerCase(JLocale.ROOT)}>"

  override def toString: String = toString(this, "")
  private def toString(node: RecordSchemaNode, tabs: String): String = {
    tabs + node.startingStr + {
      for {
        child <- node.children
      } yield "\n" + toString(child, tabs + "\t")
    }.mkString("") + {if (node.children.nonEmpty) "\n" + tabs else ""} + node.endingStr
  }
}
