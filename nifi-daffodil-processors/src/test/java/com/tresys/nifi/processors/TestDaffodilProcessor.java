/*
 * Copyright 2018 Tresys Technology, LLC
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

package com.tresys.nifi.processors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.tresys.nifi.util.DaffodilResources;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;


public class TestDaffodilProcessor {

    @Test
    public void testDFDLSchemaNotFound() {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "/does/not/exist.dfdl.xsd");
        testRunner.assertNotValid();
    }

    @Test
    public void testDFDLSchemaNotValid() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv-invalid.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSV() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
        assertEquals(DaffodilParse.XML_MIME_TYPE, infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testParseCSVFail() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        // trying to parse the XML file will fail, expects CSV data
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testUnparseCSV() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), DaffodilUnparse.XML_MIME_TYPE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
        assertNull(infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testUnparseCSVFail() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        // trying to unparse CSV will fail, expects an XML infoset
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testExpressionLanguage() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "${dfdl.schema}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("dfdl.schema", "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testNoCache() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilResources.CACHE_SIZE, "0");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVJson() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilParse.JSON_VALUE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.json")));
        infoset.assertContentEquals(expectedContent);
        assertEquals(DaffodilParse.JSON_MIME_TYPE, infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testUnparseCSVJson() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.JSON_VALUE);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), DaffodilUnparse.JSON_MIME_TYPE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.json"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
        assertNull(infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testParseCSVAttributeInvalid() {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilParse.ATTRIBUTE_VALUE);
        testRunner.assertNotValid();
    }

    @Test
    public void testUnparseCSVAttributeJSON() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.ATTRIBUTE_VALUE);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), DaffodilUnparse.JSON_MIME_TYPE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.json"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
        assertNull(infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testUnparseCSVAttributeXML() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.ATTRIBUTE_VALUE);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), DaffodilUnparse.XML_MIME_TYPE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
        assertNull(infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testUnparseCSVAttributeUndefined() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.ATTRIBUTE_VALUE);
        final Map<String, String> attributes = new HashMap<>();
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testUnparseCSVAttributeUnknown() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.ATTRIBUTE_VALUE);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/unknown");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        original.assertContentEquals(expectedContent);
        assertEquals("application/unknown", original.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testParseLeftOverData() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/bitlength.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/leftover.bin"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final byte[] expectedContent = Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/leftover.bin"));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseNoLeftOverData() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/bitlength.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/noleftover.bin"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/noleftover.bin.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVValidationLimited() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilResources.VALIDATION_MODE, DaffodilResources.LIMITED_VALUE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVValidationFull() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilResources.VALIDATION_MODE, DaffodilResources.FULL_VALUE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilComponents/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testTunableParameters() {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        testRunner.setProperty("+maxOccursBounds", "4");
        testRunner.enqueue("12345");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("failure");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("failure").get(0);
        infoset.assertContentEquals("12345");
    }

    @Test
    public void testExternalVariables() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilParse.JSON_VALUE);
        testRunner.setProperty("valueToOverride", "1");
        testRunner.enqueue("12345");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extVars.txt.json")));
        expectedContent = expectedContent.replaceAll("\\$expectedValue", "1");
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testConfigFile() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/digitList.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilParse.JSON_VALUE);
        testRunner.setProperty(DaffodilResources.CONFIG_FILE, "src/test/resources/TestDaffodilComponents/testConfig.xml");
        testRunner.enqueue("12345");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success");
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship("success").get(0);
        String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extVars.txt.json")));
        expectedContent = expectedContent.replaceAll("\\$expectedValue", "2");
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testNestedChoicesValidCase() {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/nestedChoices.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.JSON_VALUE);
        testRunner.enqueue("2");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship("success").get(0);
        firstInfoset.assertContentEquals("{\"root\": {\"B\": \"2\"}}");
    }

    @Test
    public void testNestedChoicesDefaultCase() {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilResources.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilComponents/nestedChoices.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.JSON_VALUE);
        testRunner.enqueue("4");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("success", 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship("success").get(0);
        firstInfoset.assertContentEquals("{\"root\": {\"D\": \"4\"}}");
    }

    @Test
    public void testCacheKey() {
        List<String> schemas = Arrays.asList("a", "b");
        List<String> validationModes = Arrays.asList("a", "b");
        List<HashMap<String, String>> extVarMaps = Arrays.asList(new HashMap<>(), new HashMap<>());
        extVarMaps.get(0).put("a", "b");
        extVarMaps.get(1).put("a", "c");
        List<HashMap<String, String>> tunableMaps = Arrays.asList(new HashMap<>(), new HashMap<>());
        tunableMaps.get(0).put("a", "b");
        tunableMaps.get(1).put("a", "c");
        List<String> configFiles = Arrays.asList("a", "b");

        DaffodilResources.CacheKey baseCacheKey = new DaffodilResources.CacheKey(
                schemas.get(0), validationModes.get(0), extVarMaps.get(0),
                tunableMaps.get(0), configFiles.get(0)
        );

        for (int i = 0; i < 32; i++) {
            String schema = schemas.get(i & 1);
            String validationMode = validationModes.get((i & 0b10) >> 1);
            HashMap<String, String> extVarMap = extVarMaps.get((i & 0b100) >> 2);
            HashMap<String, String> tunableMap = tunableMaps.get((i & 0b1000) >> 3);
            String configFile = configFiles.get((i & 0b10000) >> 4);

            DaffodilResources.CacheKey otherCacheKey = new DaffodilResources.CacheKey(schema, validationMode, extVarMap, tunableMap, configFile);

            assertNotSame(baseCacheKey, otherCacheKey);
            if (i == 0) {
                assertEquals(baseCacheKey, otherCacheKey);
                assertEquals(baseCacheKey.hashCode(), otherCacheKey.hashCode());
            } else {
                assertNotEquals(baseCacheKey, otherCacheKey);
                assertNotEquals(baseCacheKey.hashCode(), otherCacheKey.hashCode());
            }
        }

    }

}
