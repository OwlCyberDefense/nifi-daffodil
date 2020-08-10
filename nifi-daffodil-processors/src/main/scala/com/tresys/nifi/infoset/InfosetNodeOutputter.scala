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

package com.tresys.nifi.infoset

import com.tresys.nifi.util.DaffodilProcessingException
import org.apache.daffodil.infoset.{DIArray, DIComplex, DISimple}
import org.apache.daffodil.japi.infoset.InfosetOutputter
import org.apache.daffodil.util.MStackOf

class InfosetNodeOutputter extends InfosetOutputter {

  private val nodeStack: MStackOf[InfosetNode] = new MStackOf[InfosetNode]

  def getResult: Option[InfosetNode] = Option(nodeStack.top)

  private def addNode(node: InfosetNode): Boolean = {
    if (nodeStack.isEmpty) throw new DaffodilProcessingException("Tried to add to empty stack!")
    nodeStack.top.addChild(node)
    nodeStack.push(node)
    true
  }

  override def startSimple (diSimple: DISimple): Boolean = {
    val newNode: InfosetNode
      = new InfosetNode (diSimple.erd.name, false)
    if (diSimple.hasValue) newNode.setValue (diSimple.dataValueAsString)
    if (nodeStack.isEmpty) throw new DaffodilProcessingException("Tried to add to empty stack!")
    nodeStack.top.addChild(newNode)
    true
  }

  override def endSimple (diSimple: DISimple): Boolean = true

  override def startComplex (diComplex: DIComplex): Boolean = {
    addNode(new InfosetNode (diComplex.erd.name, false))
  }

  override def endComplex (diComplex: DIComplex): Boolean = {
    nodeStack.pop
    true
  }

  override def startArray (diArray: DIArray): Boolean = {
    addNode(new InfosetNode (diArray.erd.name, true))
  }

  override def endArray (diArray: DIArray): Boolean = {
    nodeStack.pop
    true
  }

  override def reset(): Unit = nodeStack.clear

  override def startDocument: Boolean = {
    nodeStack.push(new InfosetNode("root"))
    true
  }

  override def endDocument: Boolean = {
    if (nodeStack.isEmpty) throw new DaffodilProcessingException("Stack should not be empty after parse!")
    true
  }

  override def toString: String = if (nodeStack.isEmpty) "" else nodeStack.top.toString
}
