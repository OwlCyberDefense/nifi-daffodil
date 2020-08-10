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

package com.tresys.nifi.infoset;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Optional;

public class InfosetNode implements Iterable<InfosetNode> {
    private String name, value;
    private final LinkedList<InfosetNode> children;
    public final boolean isArray;
    private final static char[][] possibleEnclosingChars = {{'{', '}'}, {'[', ']'}};

    public InfosetNode(String name, boolean isArray) {
        this.name = name;
        this.children = new LinkedList<>();
        this.value = null;
        this.isArray = isArray;
    }

    public InfosetNode(String name) {
        this(name, false);
    }

    public String getName() {
        return name;
    }

    public void setName(String newName) {
        if (this.name.isEmpty() && newName != null && !newName.isEmpty()) {
            name = newName;
        }
    }

    public String getValue() {
        return value;
    }

    public void setValue(String newValue) {
        if (value != null) {
            throw new IllegalArgumentException("Value already set to: " + value);
        } else {
            value = newValue;
        }
    }

    public void addChild(InfosetNode newChild) {
        children.add(newChild);
    }

    @Override
    public Iterator<InfosetNode> iterator() {
        return children.iterator();
    }

    public String childrenToString() {
        return Arrays.toString(children.stream().map(InfosetNode::getName).toArray());
    }

    public Optional<InfosetNode> getChild(String childName) {
        return children.stream().filter(child -> child.getName().equals(childName)).findAny();
    }

    public String toString() {
        return toStringHelper(this, "");
    }

    private static String toStringHelper(InfosetNode node, String tabs) {
        StringBuilder toReturn = new StringBuilder(tabs);
        toReturn.append(node.name);
        toReturn.append(possibleEnclosingChars[node.isArray ? 1 : 0][0]).append("\n");
        if (node.value != null) {
            toReturn.append(tabs).append("\t").append(node.value);
            if (!node.children.isEmpty()) {
                toReturn.append(",");
            }
            toReturn.append("\n");
        }
        if (!node.children.isEmpty()) {
            for(InfosetNode child: node.children) {
                toReturn.append(toStringHelper(child, tabs + "\t"));
            }
        }
        toReturn.append(tabs).append(possibleEnclosingChars[node.isArray ? 1 : 0][1]).append("\n");
        return toReturn.toString();
    }

}
