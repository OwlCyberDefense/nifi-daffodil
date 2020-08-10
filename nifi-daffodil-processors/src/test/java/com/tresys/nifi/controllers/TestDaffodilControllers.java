/*
 * Copyright 2020 Tresys, LLC
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;

import com.tresys.nifi.util.DaffodilResources;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processors.standard.ConvertRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.xml.XMLReader;
import org.apache.nifi.xml.XMLRecordSetWriter;

import org.junit.Test;

public class TestDaffodilControllers {

    @Test
    public void testDFDLSchemaNotFound() throws InitializationException {
        final TestRunner testRunner = TestRunners.newTestRunner(ConvertRecord.class);
        ControllerService daffodilReader = new DaffodilReader();
        testRunner.addControllerService("daffodilReader", daffodilReader);
        testRunner.setProperty(daffodilReader, DaffodilResources.DFDL_SCHEMA_FILE.getName(), "/does/not/exist.dfdl.xsd");
        ControllerService daffodilWriter = new DaffodilWriter();
        testRunner.addControllerService("daffodilWriter", daffodilWriter);
        testRunner.setProperty(daffodilWriter, DaffodilResources.DFDL_SCHEMA_FILE.getName(), "/does/not/exist.dfdl.xsd");
        testRunner.setProperty("record-reader", "daffodilReader");
        testRunner.setProperty("record-writer", "daffodilWriter");
        testRunner.assertNotValid();
    }

    private enum ControllerOption {
        XML_READER,
        XML_WRITER,
        JSON_READER,
        JSON_WRITER
    }

    private TestRunner setupRunner(ControllerOption option, boolean expressionLanguage, boolean safeXMLSetup,
                                   Map<String, String> propertyMapping) throws InitializationException {
        TestRunner testRunner = TestRunners.newTestRunner(ConvertRecord.class);
        switch (option) {
            case XML_READER: {
                ControllerService xmlReader = new XMLReader();
                testRunner.addControllerService("xmlReader", xmlReader);
                if (safeXMLSetup) {
                    testRunner.setProperty(xmlReader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY);
                }
                testRunner.enableControllerService(xmlReader);
                testRunner.setProperty("record-reader", "xmlReader");
                ControllerService daffodilWriter = new DaffodilWriter();
                testRunner.addControllerService("daffodilWriter", daffodilWriter);
                propertyMapping.forEach((name, value) -> testRunner.setProperty(daffodilWriter, name, value));
                testRunner.enableControllerService(daffodilWriter);
                testRunner.setProperty("record-writer", "daffodilWriter");
                break;
            }
            case JSON_READER: {
                ControllerService jsonReader = new JsonTreeReader();
                testRunner.addControllerService("jsonReader", jsonReader);
                testRunner.enableControllerService(jsonReader);
                testRunner.setProperty("record-reader", "jsonReader");
                ControllerService daffodilWriter = new DaffodilWriter();
                testRunner.addControllerService("daffodilWriter", daffodilWriter);
                propertyMapping.forEach((name, value) -> testRunner.setProperty(daffodilWriter, name, value));
                testRunner.enableControllerService(daffodilWriter);
                testRunner.setProperty("record-writer", "daffodilWriter");
                break;
            }
            case XML_WRITER: {
                ControllerService xmlWriter = new XMLRecordSetWriter();
                testRunner.addControllerService("xmlWriter", xmlWriter);
                if (safeXMLSetup) {
                    testRunner.setProperty(xmlWriter, XMLRecordSetWriter.ROOT_TAG_NAME, "Record");
                }
                testRunner.enableControllerService(xmlWriter);
                testRunner.setProperty("record-writer", "xmlWriter");
                ControllerService daffodilReader = new DaffodilReader();
                testRunner.addControllerService("daffodilReader", daffodilReader);
                propertyMapping.forEach((descriptor, value) -> testRunner.setProperty(daffodilReader, descriptor, value));
                testRunner.enableControllerService(daffodilReader);
                testRunner.setProperty("record-reader", "daffodilReader");
                break;
            }
            default:
                ControllerService jsonWriter = new JsonRecordSetWriter();
                testRunner.addControllerService("jsonWriter", jsonWriter);
                testRunner.enableControllerService(jsonWriter);
                testRunner.setProperty("record-writer", "jsonWriter");
                ControllerService daffodilReader = new DaffodilReader();
                testRunner.addControllerService("daffodilReader", daffodilReader);
                propertyMapping.forEach((descriptor, value) -> testRunner.setProperty(daffodilReader, descriptor, value));
                testRunner.enableControllerService(daffodilReader);
                testRunner.setProperty("record-reader", "daffodilReader");
        }
        testRunner.setValidateExpressionUsage(expressionLanguage);
        return testRunner;
    }

    private TestRunner setupRunner(ControllerOption option, boolean expressionLanguage,
                                   Map<String, String> propertyMapping) throws InitializationException {
        return setupRunner(option, expressionLanguage, false, propertyMapping);
    }

    @Test
    public void testDFDLSchemaNotValid() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv-invalid.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile original = testRunner.getFlowFilesForRelationship("failure").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSV() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVFail() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        // trying to parse the XML file will fail, expects CSV data
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("failure").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testUnparseCSV() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_READER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
    }

    @Test
    public void testUnparseCSVFail() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_READER, false, valueMap);
        // trying to unparse CSV will fail, expects an XML infoset
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("failure").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testExpressionLanguage() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "${dfdl.schema}");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, true, valueMap);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("dfdl.schema", "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testNoCache() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        valueMap.put(DaffodilResources.CACHE_SIZE.getName(), "0");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVJson() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.json")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testUnparseCSVJson() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_READER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.json"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
    }

    @Test
    public void testUnparseCSVAttributeJSON() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_READER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.json"), new HashMap<>());
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile csvFile = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        csvFile.assertContentEquals(expectedContent.replace("\r\n", "\n"));
    }

    @Test
    public void testUnparseCSVAttributeXML() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_READER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilControllers/tokens.csv.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
    }

    @Test
    public void testParseLeftOverData() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/bitlength.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/leftover.bin"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile original = testRunner.getFlowFilesForRelationship("failure").get(0);
        final byte[] expectedContent = Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/leftover.bin"));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseNoLeftOverData() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/bitlength.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/noleftover.bin"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/noleftover.bin.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVValidationLimited() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        valueMap.put(DaffodilResources.VALIDATION_MODE.getName(), DaffodilResources.LIMITED_VALUE);
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("failure").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVValidationFull() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        valueMap.put(DaffodilResources.VALIDATION_MODE.getName(), DaffodilResources.FULL_VALUE);
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("failure").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testTunableParameters() throws InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        valueMap.put("+maxOccursBounds", "4");
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, valueMap);
        testRunner.enqueue("12345");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("failure").get(0);
        infoset.assertContentEquals("12345");
    }

    @Test
    public void testExternalVariables() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        valueMap.put("valueToOverride", "1");
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false, valueMap);
        testRunner.enqueue("12345");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/simpleList-extVars.txt.json")));
        expectedContent = expectedContent.replaceAll("\"\\$expectedValue\"", "1");
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testConfigFile() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        valueMap.put(DaffodilResources.CONFIG_FILE.getName(), "src/test/resources/TestDaffodilComponents/testConfig.xml");
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false, valueMap);
        testRunner.enqueue("12345");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/simpleList-extVars.txt.json")));
        expectedContent = expectedContent.replaceAll("\"\\$expectedValue\"", "2");
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testStreamModeForJsonParse() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        valueMap.put("stream-mode", AbstractDaffodilController.STREAM_MODE_ALL_SUCCESSFUL.getValue());
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false, valueMap);
        testRunner.enqueue("123456789");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship("success").get(0);
        String firstExpected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/simpleList-extraData.txt.json")));
        firstInfoset.assertContentEquals(firstExpected);
    }

    @Test
    public void testStreamModeForJsonUnparse() throws IOException, InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        valueMap.put("stream-mode", AbstractDaffodilController.STREAM_MODE_ALL_SUCCESSFUL.getValue());
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_READER, false, valueMap);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilControllers/simpleList-extraData.txt.json"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship("success").get(0);
        firstInfoset.assertContentEquals("123456789");
    }

    @Test
    public void testStreamModeForXMLParseUnparse() throws InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        valueMap.put("stream-mode", AbstractDaffodilController.STREAM_MODE_ALL_SUCCESSFUL.getValue());
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, true, valueMap);
        testRunner.enqueue("123456789");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
        final MockFlowFile infosetBytes = testRunner.getFlowFilesForRelationship("success").get(0);
        final TestRunner testRunner2 = setupRunner(ControllerOption.XML_READER, false, true, valueMap);
        testRunner2.enqueue(infosetBytes);
        testRunner2.run();
        testRunner2.assertAllFlowFilesTransferred("success", 1);
        final MockFlowFile unparsedData = testRunner2.getFlowFilesForRelationship("success").get(0);
        unparsedData.assertContentEquals("123456789");
    }

    @Test
    public void testSecondRecordFailsAllSuccessful() throws InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        valueMap.put("stream-mode", AbstractDaffodilController.STREAM_MODE_ALL_SUCCESSFUL.getValue());
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false, valueMap);
        testRunner.enqueue("1234567a9");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("failure").get(0);
        infoset.assertContentEquals("1234567a9");
    }

    @Test
    public void testSecondRecordFailsOnlySuccessful() throws InitializationException, IOException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        valueMap.put("stream-mode", AbstractDaffodilController.STREAM_MODE_ONLY_SUCCESSFUL.getValue());
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false, valueMap);
        testRunner.enqueue("1234567a9");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilControllers/simpleList-extraData.txt.json")));
        expectedContent = expectedContent.replaceAll(",8,9", "");
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testNestedChoicesValidCase() throws InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/nestedChoices.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false, valueMap);
        testRunner.enqueue("2");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship("success").get(0);
        firstInfoset.assertContentEquals("[{\"root\":{\"B\":2}}]");
    }

    @Test
    public void testNestedChoicesDefaultCase() throws InitializationException {
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(DaffodilResources.DFDL_SCHEMA_FILE.getName(), "src/test/resources/TestDaffodilComponents/nestedChoices.dfdl.xsd");
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false, valueMap);
        testRunner.enqueue("4");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship("success").get(0);
        firstInfoset.assertContentEquals("[{\"root\":{\"D\":4}}]");
    }

}
