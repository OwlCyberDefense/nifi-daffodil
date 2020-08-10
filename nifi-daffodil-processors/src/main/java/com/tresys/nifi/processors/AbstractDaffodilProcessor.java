/*
 * Copyright 2020 Tresys Technology, LLC
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

import com.tresys.nifi.util.DaffodilResources;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.tresys.nifi.util.DaffodilResources.*;

public abstract class AbstractDaffodilProcessor extends AbstractProcessor {

    abstract protected void processWithDaffodil(final DataProcessor dp,
                                                final FlowFile ff, final InputStream in, final OutputStream out,
                                                String infosetType) throws IOException;

    /**
     * Returns the mime type of the resulting output FlowFile. If the mime type
     * cannot be determine, this should return null, and the mime.type
     * attribute will be removed from the output FlowFile.
     */
    protected abstract String getOutputMimeType(String infosetType);
    protected abstract boolean isUnparse();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully transformed will be routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();

    /**
     * This is not static like the other PropertyDescriptors. This is because
     * the allowable values differ based on whether this is parse or unparse
     * (mime.type attribute is not allow for parse). So on init() we will
     * create this property descriptor accordingly.
     */
    private PropertyDescriptor INFOSET_TYPE = null;
    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<AllowableValue> allowableInfosetTypeValues = new ArrayList<>(Arrays.asList(INFOSET_TYPE_XML, INFOSET_TYPE_JSON));
        if (isUnparse()) {
            // using the mime type for infoset type only applies to unparse
            allowableInfosetTypeValues.add(INFOSET_TYPE_ATTRIBUTE);
        }

        INFOSET_TYPE = new PropertyDescriptor.Builder()
                .name("infoset-type")
                .displayName("Infoset Type")
                .description("The format of the FlowFile to output (for parsing) or input (for unparsing).")
                .required(true)
                .defaultValue(INFOSET_TYPE_XML.getValue())
                .allowableValues(allowableInfosetTypeValues.toArray(new AllowableValue[0]))
                .build();

        final List<PropertyDescriptor> properties = new ArrayList<>(DaffodilResources.daffodilProperties);
        properties.add(INFOSET_TYPE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return DaffodilResources.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        DaffodilResources.buildCache(getLogger(), context);
    }

    static final String XML_MIME_TYPE = "application/xml";
    static final String JSON_MIME_TYPE = "application/json";

    static final String XML_VALUE = "xml";
    static final String JSON_VALUE = "json";
    static final String ATTRIBUTE_VALUE = "use mime.type attribute";

    static final AllowableValue INFOSET_TYPE_XML = new AllowableValue(XML_VALUE, XML_VALUE, "The FlowFile representation is XML");
    static final AllowableValue INFOSET_TYPE_JSON = new AllowableValue(JSON_VALUE, JSON_VALUE, "The FlowFile representation is JSON");
    static final AllowableValue INFOSET_TYPE_ATTRIBUTE = new AllowableValue(ATTRIBUTE_VALUE, ATTRIBUTE_VALUE, "The FlowFile representation is determined based on the mime.type attribute");

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
        final String dfdlSchema = context.getProperty(DaffodilResources.DFDL_SCHEMA_FILE).evaluateAttributeExpressions(original).getValue();
        String infosetTypeValue = context.getProperty(INFOSET_TYPE).getValue();
        final String infosetType;

        if (infosetTypeValue.equals(ATTRIBUTE_VALUE)) {
            if (!isUnparse()) {
                throw new AssertionError("infoset type 'attribute' should only occur with Daffodil unparse");
            }
            String inputMimeType = original.getAttribute(CoreAttributes.MIME_TYPE.key());
            switch (inputMimeType == null ? "" : inputMimeType) {
                case XML_MIME_TYPE: infosetType = XML_VALUE; break;
                case JSON_MIME_TYPE: infosetType = JSON_VALUE; break;
                default:
                    logger.error("Infoset Type is 'attribute', but the mime.type attribute is not set or not recognized for {}.", new Object[]{original});
                    session.transfer(original, REL_FAILURE);
                    return;
            }
        } else {
            infosetType = infosetTypeValue;
        }

        try {
            FlowFile output = session.write(original,
                (in, out) -> {
                    DataProcessorSchemaPair pair =
                        DaffodilResources.getDataProcessorSchemaPair(getLogger(), context, dfdlSchema);
                    DataProcessor dp = pair.getDataProcessor();
                    processWithDaffodil(dp, original, in, out, infosetType);
                }
            );
            final String outputMimeType = getOutputMimeType(infosetType);
            if (outputMimeType != null) {
                output = session.putAttribute(output, CoreAttributes.MIME_TYPE.key(), outputMimeType);
            } else {
                output = session.removeAttribute(output, CoreAttributes.MIME_TYPE.key());
            }
            session.transfer(output, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(output, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.debug("Processed {}", new Object[]{original});
        } catch (ProcessException e) {
            logger.error("Failed to process {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        }
    }

}
