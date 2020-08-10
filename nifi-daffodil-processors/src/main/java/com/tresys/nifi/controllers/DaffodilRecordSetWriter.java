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
import com.tresys.nifi.infoset.InfosetNodeInputter;
import com.tresys.nifi.util.DaffodilProcessingException;
import com.tresys.nifi.schema.OptionalRecordField;
import com.tresys.nifi.schema.RecordUtil;
import org.apache.daffodil.japi.UnparseResult;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DaffodilRecordSetWriter implements RecordSetWriter {

    private final ComponentLog logger;
    private final DataProcessor dataProcessor;
    private final OutputStream outputStream;
    private final RecordSchema originalSchema;
    private WriteResult result;
    private final StreamMode streamMode;

    public DaffodilRecordSetWriter(DataProcessor dataProcessor, OutputStream outputStream,
                                   RecordSchema originalSchema, StreamMode streamMode, ComponentLog logger) {
        this.logger = logger;
        this.dataProcessor = dataProcessor;
        this.outputStream = outputStream;
        this.originalSchema = originalSchema;
        this.result = WriteResult.EMPTY;
        this.streamMode = streamMode;
    }

    /**
     * Note that nothing involving a RecordSet is supported in this class; only individual Records.
     * However, this class IS able to support multiple Records as input; these simply are processed by multiple
     * calls to write(Record) - you can see this by how the RecordSetWriter is used in AbstractDaffodilProcessor's
     * onTrigger implementation
     */

    @Override
    public WriteResult write(RecordSet recordSet) {
        throw new UnsupportedOperationException(
            "This method is not supported!  Please use a Record, not a RecordSet, as a parameter to write()"
        );
    }

    @Override
    public void beginRecordSet() { }

    @Override
    public WriteResult finishRecordSet() throws DaffodilProcessingException {
        if (result == null) {
            throw new DaffodilProcessingException("Called finishRecordSet() but no valid result was obtained!");
        } else {
            return result;
        }
    }

    @Override
    public WriteResult write(Record record) throws DaffodilProcessingException {
        if (streamMode != StreamMode.OFF || result.getRecordCount() == 0) {
            try {
                InfosetNodeInputter infosetNodeInputter
                    = new InfosetNodeInputter(recordToInfosetNode(originalSchema, record.toMap(), logger), logger);
                UnparseResult unparseResult = dataProcessor.unparse(infosetNodeInputter, Channels.newChannel(outputStream));
                if (unparseResult.isError()) {
                    throw new DaffodilProcessingException(
                        String.format(
                            "Could not parse input record %s due to %s", RecordUtil.printRecord(record, ""),
                            unparseResult.getDiagnostics()
                        )
                    );
                } else {
                    this.result = WriteResult.of(this.result.getRecordCount() + 1, Collections.emptyMap());
                }
            } catch (DaffodilProcessingException possiblyIgnored) {
                if (streamMode != StreamMode.ONLY_SUCCESSFUL) {
                    throw possiblyIgnored;
                }
            }
        }
        return this.result;
    }

    /**
     * Converts a given Record into a Tree of InfosetNodes.  We only pass around the values map to helper methods,
     * instead of the original Record object directly, because we traverse the Record based on the known Schema
     * obtained from the DSOM API, *not* whatever Schema that was provided with the Record.
     * @throws DaffodilProcessingException if a required field is not found in the values Map
     */
    private static InfosetNode recordToInfosetNode(RecordSchema schema, Map<String, Object> values,
                                                   ComponentLog logger) throws DaffodilProcessingException {
        InfosetNode recordNode = new InfosetNode("", false);
        for (RecordField field: schema.getFields()) {
            try {
                List<InfosetNode> nodeResult = recordValueToNode(field.getFieldName(), field.getDataType(), values, logger);
                nodeResult.forEach(recordNode::addChild);
            } catch (DaffodilProcessingException possiblyIgnored) {
                if (!(field instanceof OptionalRecordField)) {
                    throw possiblyIgnored;
                }
            }
        }
        return recordNode;
    }

    /**
     * Given a field name, type, and map with values, returns one or more InfosetNodes representing
     * the data that corresponds to the field name that is found in the map.  Processing this data
     * is heavily informed by the passed in dataType.  Multiple InfosetNodes may be returned due to the fact
     * that processed Choices may refer to multiple extracted Field-Value pairs
     * @param fieldName the name of the field to be selected from values
     * @param dataType the dataType of the current field
     * @param values a Map containing the values from the original Record
     * @param logger a log component for debugging
     * @return a List of InfosetNodes containing the processed data from converting the given Record
     * @throws DaffodilProcessingException if fieldName cannot be found in the provided map and the dataType is not
     *                               a Choice
     */
    private static List<InfosetNode> recordValueToNode(String fieldName, DataType dataType, Map<String, Object> values,
                                                       ComponentLog logger) throws DaffodilProcessingException {
        Optional<Object> optValue = Optional.ofNullable(values.get(fieldName));
        if (!(dataType instanceof ChoiceDataType) && !optValue.isPresent()) {
            throw new DaffodilProcessingException(
                String.format(
                    "Required Schema field %s was not present in map %s", fieldName, values
                )
            );
        }
        List<InfosetNode> nodesToReturn = new ArrayList<>();
        if (dataType instanceof RecordDataType) {
            RecordDataType recordDataType = (RecordDataType) dataType;
            Record subRecord;
            if (optValue.get() instanceof Record) {
                subRecord = (Record) optValue.get();
            } else {
                throw new DaffodilProcessingException(
                    String.format("Expected a Record value, but instead got invalid value %s", optValue.get().toString())
                );
            }
            // recordToInfosetNode returns an InfosetNode with an empty name and a list of child Nodes.  To make
            // it fit here, all we have to do is change its name to fieldName.
            InfosetNode recordNode = recordToInfosetNode(recordDataType.getChildSchema(), subRecord.toMap(), logger);
            recordNode.setName(fieldName);
            nodesToReturn.add(recordNode);
        } else if (dataType instanceof ChoiceDataType) {
            ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            List<DataType> possibleTypes = choiceDataType.getPossibleSubTypes();
            nodesToReturn = choiceToInfosetNode(possibleTypes, values, logger);
        } else if (dataType instanceof ArrayDataType) {
            DataType arrayMemberType = ((ArrayDataType) dataType).getElementType();
            Object[] arrValues;
            // This was added for XML specifically; an XML reader won't know if an element was originally
            // an array of length 1 or just an element.  If the dataType is an array, then try to parse the
            // data like it was part of an array of length 1.
            if (optValue.get() instanceof Object[]) {
                arrValues = (Object[]) optValue.get();
            } else {
                arrValues = new Object[1];
                arrValues[0] = optValue.get();
            }
            List<InfosetNode> arrNodes = new ArrayList<>();
            for (Object arrMember: arrValues) {
                /*
                 * recordValueToNode normally produces a wrapper InfosetNode that has no name and just some value,
                 * which normally represents an Array value pretty accurately.
                 *
                 * However, RecordInputter will not accept any wrapper elements other than the root; it needs
                 * the actual data immediately.  So, whenever an Array of Records is present, we convert it into
                 * a List of Infoset Nodes, each of which has the Array's name and are assigned the data of what
                 * used to be the wrapper nodes.  This is exactly what Arrays look like in an XML infoset.
                 */
                Map<String, Object> subMap = new HashMap<>();
                subMap.put("", arrMember);
                for (InfosetNode recordNode: recordValueToNode("", arrayMemberType, subMap, logger)) {
                    recordNode.setName(fieldName);
                    arrNodes.add(recordNode);
                }
            }
            nodesToReturn = arrNodes;
        } else {
            InfosetNode childNode = new InfosetNode(fieldName, false);
            childNode.setValue(optValue.get().toString());
            nodesToReturn.add(childNode);
        }
        return nodesToReturn;
    }

    /**
     * Given a list of possible sub-types, extracts a value or values that is intended to be the result of a prior
     * choice parse and constructs one or more InfosetNodes encompassing these values
     * As with parse, this may be more than one value because NiFi Choices for RecordSchemas
     * must be of all RecordDataTypes, which may have multiple fields
     * @param possibleSubTypes a list of possible sub-types the Choice may take on
     * @param values a map for which one or several of the values will be extracted to comprise this Choice
     *               InfosetNode.
     * @param logger a log component for debugging
     * @return the List of InfosetNodes generated from processing the selected Choice Option's values
     * @throws DaffodilProcessingException if one of the possibleSubTypes is not a RecordDataType, or if none
     *                               of the choice options are successfully selected.
     */
    private static List<InfosetNode> choiceToInfosetNode(List<DataType> possibleSubTypes, Map<String, Object> values,
                                                         ComponentLog logger) throws DaffodilProcessingException {
        List<InfosetNode> children = new ArrayList<>();
        for (DataType possibleType: possibleSubTypes) {
            if (!(possibleType instanceof RecordDataType)) {
                throw new DaffodilProcessingException("Possible Type of Choice element was not a record!");
            } else {
                RecordDataType possRecordType = (RecordDataType) possibleType;
                List<RecordField> allFields = possRecordType.getChildSchema().getFields();
                // fieldsPresent will contain *all* fields we are able to find, whereas allFound
                // will only be false if a required (non-optional) field is not found.
                List<RecordField> fieldsPresent = new ArrayList<>();
                boolean allFound = true;
                for (RecordField field: allFields) {
                    if (values.keySet().stream().noneMatch(name -> name.equals(field.getFieldName()))) {
                        if (!(field instanceof OptionalRecordField)) {
                            allFound = false;
                            break;
                        }
                    } else {
                        fieldsPresent.add(field);
                    }
                }
                if (allFound) {
                    for (RecordField field: fieldsPresent) {
                        // recordValueToNode may still return multiple InfosetNodes because of nested choices!
                        // (hence why we have to do addAll, not just add here)
                        children.addAll(recordValueToNode(field.getFieldName(), field.getDataType(), values, logger));
                    }
                }
            }
        }
        if (children.isEmpty()) {
            throw new DaffodilProcessingException(
                String.format("InfosetNode Child List %s did not match any choice option of choice %s",
                    possibleSubTypes.toString(), values
                )
            );
        }
        return children;
    }

    @Override
    public String getMimeType() {
        return "text/plain";
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}
