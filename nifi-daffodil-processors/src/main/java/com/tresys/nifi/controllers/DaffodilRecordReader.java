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

package com.tresys.nifi.controllers;

import com.tresys.nifi.controllers.AbstractDaffodilController.StreamMode;
import com.tresys.nifi.infoset.InfosetNode;
import com.tresys.nifi.infoset.InfosetNodeOutputter;
import com.tresys.nifi.schema.OptionalRecordField;
import com.tresys.nifi.util.DaffodilProcessingException;
import org.apache.daffodil.japi.DataLocation;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.ParseResult;
import org.apache.daffodil.japi.io.InputSourceDataInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DaffodilRecordReader implements RecordReader {

    private final DataProcessor dataProcessor;
    private final RecordSchema originalSchema;
    private RecordSchema currentSchema;
    private final InputStream inputStream;
    private final InputSourceDataInputStream dataInputStream;
    private long totalBits, bitsRead;
    private boolean coerceTypes;
    private final ComponentLog logger;
    private final StreamMode streamMode;

    public DaffodilRecordReader(RecordSchema schema, InputStream inputStream, DataProcessor dataProcessor, StreamMode streamMode, ComponentLog logger) {
        this.originalSchema = schema;
        this.coerceTypes = false;
        this.inputStream = inputStream;
        this.dataInputStream = new InputSourceDataInputStream(inputStream);
        this.totalBits = -1;
        this.bitsRead = 0;
        this.dataProcessor = dataProcessor;
        this.logger = logger;
        this.currentSchema = originalSchema;
        this.streamMode = streamMode;
    }

    @Override
    public Record nextRecord(boolean shouldCoerceTypes, boolean ignored) throws IOException {
        this.coerceTypes = shouldCoerceTypes;
        if (inputStream.available() > 0) {
            this.totalBits = inputStream.available() * 8;
        }
        if (inputStream.available() > 0 || (streamMode != StreamMode.OFF && bitsRead < totalBits)) {
            try {
                InfosetNode rootNode = parseToInfosetNode();
                Record result = complexNodeToRecord(originalSchema, rootNode);
                currentSchema = result.getSchema();
                return result;
            } catch (DaffodilProcessingException possiblyIgnored) {
                if (streamMode != StreamMode.ONLY_SUCCESSFUL) {
                    logger.error("Error Performing Daffodil Parse: {}", new Object[]{possiblyIgnored});
                    throw possiblyIgnored;
                }
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * Perform a parse using the given DataProcessor to an Outputter that outputs
     * the Infoset data in the form of an InfosetNode tree
     * @return the InfosetNode result of the parse
     */
    private InfosetNode parseToInfosetNode() throws DaffodilProcessingException {
        InfosetNodeOutputter outputter = new InfosetNodeOutputter();
        ParseResult result = dataProcessor.parse(dataInputStream, outputter);
        if (result.isError()) {
            throw new DaffodilProcessingException("Failed to parse: " + result.getDiagnostics());
        }
        if (!outputter.getResult().isDefined()) {
            throw new DaffodilProcessingException("NodeOutputter did not successfully parse infoset!");
        }
        // Currently, if all bytes are not consumed and the Processor is not set to stream mode, we
        // throw an Exception and then, as a result, route to failure
        DataLocation loc = result.location();
        if (loc.bitPos1b() - 1 <= bitsRead) {
            throw new DaffodilProcessingException(
                String.format(
                    "No data consumed! The current parse started and ended with %s bit(s)"
                        + " having been read when trying to parse %s",
                    bitsRead, outputter.getResult().get()
                )
            );
        } else {
            bitsRead = loc.bitPos1b() - 1;
        }
        if (streamMode == StreamMode.OFF && totalBits != bitsRead) {
            throw new DaffodilProcessingException(
                String.format(
                    "Left over data. Consumed %s bit(s) with %s bit(s) remaining when parsing %s",
                    bitsRead, totalBits - bitsRead, outputter.getResult().get()
                )
            );
        }
        return outputter.getResult().get();
    }

    /**
     * Converts a given InfosetNode into a Record based on processed values returned by getRecordValue
     * @param schema the sub-RecordSchema corresponding to this Record
     * @param parentNode the InfosetNode with data corresponding to this Record
     * @return a NiFi Record with a possibly modified Schema correctly filled with the data from the Node
     */
    private Record complexNodeToRecord(RecordSchema schema, InfosetNode parentNode) throws DaffodilProcessingException {
        Map<String, Object> recordMap = new LinkedHashMap<>();
        List<RecordField> modifiedFieldList = new ArrayList<>();
        for (RecordField field: schema.getFields()) {
            // If an Optional Field does not occur in the data, it will throw an Exception at some point.
            // So, we only propagate Exceptions from fields here if they come from required fields.
            try {
                List<FieldValuePair> fieldValuePairs
                    = getRecordValue(field.getFieldName(), field.getDataType(), parentNode);
                for (FieldValuePair pair: fieldValuePairs) {
                    modifiedFieldList.add(pair.toRecordField());
                    recordMap.put(pair.name, pair.value);
                }
            } catch (DaffodilProcessingException possiblyIgnored) {
                if (!(field instanceof OptionalRecordField)) {
                    throw possiblyIgnored;
                }
            }
        }
        SimpleRecordSchema newSchema = new SimpleRecordSchema(modifiedFieldList);
        /* The XML Writer will need some sort of name for the Root tag encompassing all the Infoset data
         * If we set the schema name, then it will just use it as the Root tag name instead of having
         * to be configured with an explicit one
         */
        schema.getSchemaName().ifPresent(newSchema::setSchemaName);
        return new MapRecord(newSchema, recordMap);
    }

    /**
     * Temporary Wrapper class representing a field-value pair
     * Used here because anonymous choices may be transformed into multiple fields
     */
    private static class FieldValuePair {
        public final String name;
        public final DataType type;
        public final Object value;

        public FieldValuePair(String name, DataType type, Object value) {
            this.name = name;
            this.type = type;
            this.value = value;
        }

        public RecordField toRecordField() {
            return new RecordField(name, type);
        }
    }

    /**
     * Searches for the given child based on `fieldName` in the child list of parentNode, and then
     * proceeds to convert that Node into a temporary list of FieldValuePairs that will later be converted
     * into RecordFields and have the data extracted from them.
     * The reason why we don't simply pass the child InfosetNode to this method is that the particular
     * complicated for anonymous choices; the child that was selected by the choice isn't known until we invoke
     * choiceToPairList
     * @param fieldName the name of the child Node/field we are searching for
     * @param dataType the type of the current field
     * @param parentNode the parent of the list that the current child Node is in
     * @return a list of Field-Value Pairs to insert into the Record containing this field
     */
    private List<FieldValuePair> getRecordValue(String fieldName, DataType dataType,
                                                InfosetNode parentNode) throws DaffodilProcessingException {
        List<FieldValuePair> fieldValuePairs = new ArrayList<>();
        Optional<InfosetNode> optChild = parentNode.getChild(fieldName);
        if (fieldName == null) {
            // when no field name is given, we treat the "parentNode" like it's a child node, and dataType matches its
            // type instead of the "child"'s type
            optChild = Optional.of(parentNode);
        }
        if (!(dataType instanceof ChoiceDataType) && !optChild.isPresent()) {
            throw new DaffodilProcessingException(
                String.format(
                    "Required Schema field %s was not present in child list %s", fieldName, parentNode.childrenToString()
                )
            );
        }
        if (dataType instanceof RecordDataType) {
            RecordDataType recordDataType = (RecordDataType) dataType;
            Record subRecord = complexNodeToRecord(recordDataType.getChildSchema(), optChild.get());
            dataType = RecordFieldType.RECORD.getRecordDataType(subRecord.getSchema());
            fieldValuePairs.add(new FieldValuePair(fieldName, dataType, subRecord));
        } else if (dataType instanceof ChoiceDataType) {
            ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            List<DataType> possibleTypes = choiceDataType.getPossibleSubTypes();
            return choiceToPairList(possibleTypes, parentNode);
        } else if (dataType instanceof ArrayDataType) {
            DataType arrayMemberType = ((ArrayDataType) dataType).getElementType();
            fieldValuePairs.add(arrayToPair(fieldName, arrayMemberType, optChild.get()));
        } else {
            Object simpleValue;
            if (coerceTypes) {
                simpleValue = coerceSimpleType(dataType.getFieldType(), optChild.get().getValue());
            } else {
                simpleValue = optChild.get().getValue();
            }
            fieldValuePairs.add(new FieldValuePair(fieldName, dataType, simpleValue));
        }
        return fieldValuePairs;
    }

    /**
     * Given a parent InfosetNode, iterates through the parent's child list and tries to process one
     * that matches one of the given sub-types.
     * There are potentially multiple Pairs returned here because a Choice option may be an element
     * with multiple fields; note the plural nature of the "allFields" variable.
     * @param possibleSubTypes the possible DataTypes this Choice could be.  An error is thrown if these
     *                         are not all RecordDataTypes
     * @param parentNode the parent InfosetNode whose child list should contain the Choice option to be selected
     * @return a list of Pairs representing the Fields & Values that were selected for the Choice
     * @throws DaffodilProcessingException if one of the possibleSubTypes is not a RecordDataType, or if none
     *                               of the choice options are successfully selected.
     */
    private List<FieldValuePair> choiceToPairList(List<DataType> possibleSubTypes,
                                                  InfosetNode parentNode) throws DaffodilProcessingException {
        List<FieldValuePair> fieldValPairs = new ArrayList<>();
        for (DataType possibleType: possibleSubTypes) {
            if (!(possibleType instanceof RecordDataType)) {
                throw new DaffodilProcessingException("Possible Type of Choice element was not a record!");
            } else {
                RecordDataType possRecordType = (RecordDataType) possibleType;
                // RecordFields belonging to the RecordSchema for the current Choice Option
                List<RecordField> allFields = possRecordType.getChildSchema().getFields();
                List<RecordField> fieldsFound = new ArrayList<>();
                boolean allFound = true;

                for (RecordField field: allFields) {
                    if (!parentNode.getChild(field.getFieldName()).isPresent()) {
                        if (!(field instanceof OptionalRecordField)) {
                            allFound = false;
                        }
                    } else {
                        fieldsFound.add(field);
                    }
                }
                if (allFound) {
                    for (RecordField field: fieldsFound) {
                        fieldValPairs.addAll(getRecordValue(field.getFieldName(), field.getDataType(), parentNode));
                    }
                }
            }
        }
        if (fieldValPairs.isEmpty()) {
            throw new DaffodilProcessingException(
                String.format("InfosetNode Child List %s did not match any choice option of choice %s",
                    possibleSubTypes.toString(), parentNode.toString()
                )
            );
        }
        return fieldValPairs;
    }

    /**
     * Given a parent Array Node, create a Pair that has the name of the Array and the dataType is
     * an Array of Choices; the Choice being of every single possible type in the Array.  This is necessary
     * because anonymous choices change the RecordDataType manifested originally, so an Array that has an
     * anonymous choice as some sub-element would need to know this.
     * @param originalFieldName the name of this Array
     * @param childType the original element type of the Array; note that this is not necessarily the same
     *                  as the element type of the final Array, as the final dataType is obtained via pair.type
     * @param arrayNode the InfosetNode representing the current Array
     * @return a FieldValuePair as described above
     */
    private FieldValuePair arrayToPair(String originalFieldName, DataType childType,
                                       InfosetNode arrayNode) throws DaffodilProcessingException {
        List<Object> valueArrList = new ArrayList<>();
        List<DataType> possibleSubTypes = new ArrayList<>();
        for (InfosetNode arrayMember : arrayNode) {
            for (FieldValuePair pair : getRecordValue(null, childType, arrayMember)) {
                valueArrList.add(pair.value);
                possibleSubTypes.add(pair.type);
            }
        }
        DataType choiceType = RecordFieldType.CHOICE.getChoiceDataType(possibleSubTypes);
        return new FieldValuePair(
            originalFieldName, RecordFieldType.ARRAY.getArrayDataType(choiceType), valueArrList.toArray()
        );
    }

    /**
     * Attempts to parse a String into another simple type
     * @param type the type that the String is attempting to parse into
     * @param simpleTypeValue the String to be parsed
     * @return the parsed version of the String, if it succeeds
     * @throws DaffodilProcessingException if type is not one of the known types or the parse fails
     */
    private Object coerceSimpleType(RecordFieldType type, String simpleTypeValue) throws DaffodilProcessingException {
        try {
            switch (type) {
                case STRING:
                    return simpleTypeValue;
                case BYTE:
                    return Byte.parseByte(simpleTypeValue);
                case SHORT:
                    return Short.parseShort(simpleTypeValue);
                case INT:
                    return Integer.parseInt(simpleTypeValue);
                case LONG:
                    return Long.parseLong(simpleTypeValue);
                case BIGINT:
                    return new BigInteger(simpleTypeValue);
                default:
                    throw new DaffodilProcessingException("Attempted coercing unsupported type " + type);
            }
        } catch (NumberFormatException nfe) {
            throw new DaffodilProcessingException(String.format("Could not cast %s to a %s", simpleTypeValue, type.name()));
        }
    }

    @Override
    public RecordSchema getSchema() {
        return currentSchema;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
