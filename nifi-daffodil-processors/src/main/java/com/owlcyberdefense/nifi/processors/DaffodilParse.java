/*
 * Copyright 2021 Owl Cyber Defense
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

package com.owlcyberdefense.nifi.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;

import org.apache.daffodil.japi.DataLocation;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.ParseResult;
import org.apache.daffodil.japi.io.InputSourceDataInputStream;
import org.apache.daffodil.japi.infoset.InfosetOutputter;
import org.apache.daffodil.japi.infoset.JsonInfosetOutputter;
import org.apache.daffodil.japi.infoset.XMLTextInfosetOutputter;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"xml", "json", "daffodil", "dfdl", "schema", "xsd"})
@CapabilityDescription("Use Daffodil and a user-specified DFDL schema to transform data to an infoset, represented by either XML or JSON.")
@WritesAttribute(attribute = "mime.type", description = "Sets the mime type to application/json or application/xml based on the infoset type.")
@DynamicProperty(
    name = "Name of external variable defined in a DFDL schema",
    value = "Value to set for the DFDL external variable. May be an expression. The DFDL variable is not set if the value expression evaluates to an empty string.",
    description = "Defines an external variable to be used when parsing",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
)
@RequiresInstanceClassLoading
public class DaffodilParse extends AbstractDaffodilProcessor {

    private InfosetOutputter getInfosetOutputter(String infosetType, OutputStream os) {
        switch (infosetType) {
            case XML_VALUE: return new XMLTextInfosetOutputter(os, false);
            case JSON_VALUE: return new JsonInfosetOutputter(os, false);
            default: throw new AssertionError("Unhandled infoset type: " + infosetType);
        }
    }

    @Override
    protected boolean isUnparse() { return false; }

    @Override
    protected String getOutputMimeType(String infosetType) {
        switch (infosetType) {
            case XML_VALUE: return XML_MIME_TYPE;
            case JSON_VALUE: return JSON_MIME_TYPE;
            default: throw new AssertionError("Unhandled infoset type: " + infosetType);
        }
    }

    @Override
    protected void processWithDaffodil(final DataProcessor dp, final FlowFile ff, final InputStream in, final OutputStream out, String infosetType) throws IOException {
        InputSourceDataInputStream input = new InputSourceDataInputStream(in);
        InfosetOutputter outputter = getInfosetOutputter(infosetType, out);
        ParseResult pr = dp.parse(input, outputter);
        if (pr.isError()) {
            getLogger().error("Failed to parse {}", new Object[]{ff});
            logDiagnostics(getLogger(), pr);
            throw new DaffodilProcessingException("Failed to parse");
        }
        DataLocation loc = pr.location();
        long bitsRead = loc.bitPos1b() - 1;
        long expectedBits = ff.getSize() * 8;
        if (expectedBits != bitsRead) {
            getLogger().error("Left over data. Consumed {} bit(s) with {} bit(s) remaining when parsing {}", new Object[]{bitsRead, expectedBits - bitsRead, ff});
            throw new DaffodilProcessingException("Left over data found");
        }
        out.flush();
    }

}
