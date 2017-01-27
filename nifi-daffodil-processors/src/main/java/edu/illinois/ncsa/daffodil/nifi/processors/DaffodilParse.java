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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;

import org.jdom2.Document;
import org.jdom2.output.XMLOutputter;

import edu.illinois.ncsa.daffodil.japi.DataProcessor;
import edu.illinois.ncsa.daffodil.japi.ParseResult;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"xml", "daffodil", "dfdl", "schema", "xsd"})
@CapabilityDescription("Use Daffodil and a user-specified DFDL schema to parse data to an XML infoset.")
public class DaffodilParse extends AbstractDaffodilProcessor {

    @Override
    protected void processWithDaffodil(final DataProcessor dp, final FlowFile ff, final InputStream rawIn, final OutputStream out) throws IOException {
        ReadableByteChannel rbc = Channels.newChannel(rawIn);
        ParseResult pr = dp.parse(rbc, ff.getSize() * 8);
        if (pr.isError()) {
            getLogger().error("Failed to parse {}", new Object[]{ff});
            logDiagnostics(pr);
            throw new DaffodilProcessingException("Failed to parse");
        }
        Document doc = pr.result();
        XMLOutputter xmlOut = new XMLOutputter();
        xmlOut.output(doc, out);
    }

}
