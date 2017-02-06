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
import java.nio.channels.WritableByteChannel;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;

import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import edu.illinois.ncsa.daffodil.japi.DataProcessor;
import edu.illinois.ncsa.daffodil.japi.UnparseResult;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"xml", "daffodil", "dfdl", "schema", "xsd"})
@CapabilityDescription("Use Daffodil and a user-specified DFDL schema to transform an XML representation of data back to the original data format.")
public class DaffodilUnparse extends AbstractDaffodilProcessor {

    @Override
    protected void processWithDaffodil(final DataProcessor dp, final FlowFile ff, final InputStream rawIn, final OutputStream out) throws IOException {
        try {
            SAXBuilder sb = new SAXBuilder();
            Document doc = sb.build(rawIn);
            WritableByteChannel wbc = Channels.newChannel(out);
            UnparseResult ur = dp.unparse(wbc, doc);
            if (ur.isError()) {
                getLogger().error("Failed to unparse {}", new Object[]{ff});
                logDiagnostics(ur);
                throw new DaffodilProcessingException("Failed to unparse");
            }
        } catch (JDOMException e) {
            throw new IOException(e);
        }
    }

}

