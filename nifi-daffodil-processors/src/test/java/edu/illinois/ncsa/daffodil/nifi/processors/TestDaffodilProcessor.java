/*
 * Copyright 2017 Tresys Technology, LLC
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

package edu.illinois.ncsa.daffodil.nifi.processors;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class TestDaffodilProcessor {

    @Test
    public void testDFDLSchemaNotFound() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "/does/not/exist.dfdl.xsd");
        testRunner.assertNotValid();
    }

    @Test
    public void testDFDLSchemaNotValid() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv-invalid.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSV() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
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
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        // trying to parse the XML file will fail, expects CSV data
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVPreCompiled() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd.bin");
        testRunner.setProperty(DaffodilParse.PRE_COMPILED_SCHEMA, "true");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
        assertEquals(DaffodilParse.XML_MIME_TYPE, infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testParseCSVPreCompiledFail() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.PRE_COMPILED_SCHEMA, "true");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testUnparseCSV() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), DaffodilUnparse.XML_MIME_TYPE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
        assertEquals(null, infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testUnparseCSVFail() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        // trying to unparse CSV will fail, expectes an XML infoset
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testExpressionLanguage() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "${dfdl.schema}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("dfdl.schema", "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testNoCache() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.CACHE_SIZE, "0");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVJson() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilParse.JSON_VALUE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
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
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.JSON_VALUE);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), DaffodilUnparse.JSON_MIME_TYPE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.json"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
        assertEquals(null, infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testParseCSVAttributeInvalid() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilParse.ATTRIBUTE_VALUE);
        testRunner.assertNotValid();
    }

    @Test
    public void testUnparseCSVAttributeJSON() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.ATTRIBUTE_VALUE);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), DaffodilUnparse.JSON_MIME_TYPE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.json"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
        assertEquals(null, infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testUnparseCSVAttributeXML() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty("infoset-type", DaffodilUnparse.ATTRIBUTE_VALUE);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), DaffodilUnparse.XML_MIME_TYPE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
        assertEquals(null, infoset.getAttribute(CoreAttributes.MIME_TYPE.key()));
    }

    @Test
    public void testUnparseCSVAttributeUndefined() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
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
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
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
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/bitlength.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/leftover.bin"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final byte[] expectedContent = Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/leftover.bin"));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseNoLeftOverData() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/bitlength.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/noleftover.bin"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/noleftover.bin.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVValidationLimited() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.VALIDATION_MODE, DaffodilParse.LIMITED_VALUE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVValidationFull() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.VALIDATION_MODE, DaffodilParse.FULL_VALUE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

}
