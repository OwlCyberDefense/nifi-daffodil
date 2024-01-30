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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.daffodil.japi.Compiler;
import org.apache.daffodil.japi.Daffodil;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.Diagnostic;
import org.apache.daffodil.japi.ExternalVariableException;
import org.apache.daffodil.japi.ProcessorFactory;
import org.apache.daffodil.japi.WithDiagnostics;
import org.apache.daffodil.japi.ValidationMode;
import org.apache.daffodil.japi.InvalidUsageException;
import org.apache.daffodil.japi.InvalidParserException;


public abstract class AbstractDaffodilProcessor extends AbstractProcessor {

    abstract protected void processWithDaffodil(final DataProcessor dp, final FlowFile ff, final InputStream in, final OutputStream out, String infosetType) throws IOException;

    /**
     * Returns the mime type of the resulting output FlowFile. If the mime type
     * cannot be determine, this should return null, and the mime.type
     * attribute will be removed from the output FlowFile.
     */
    abstract protected String getOutputMimeType(String infosetType);
    abstract protected boolean isUnparse();

    public static final PropertyDescriptor DFDL_SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("dfdl-schema-file")
            .displayName("DFDL Schema File")
            .description("Path to the DFDL schema file or resource (see 'Plugins and Schemas') used for parsing/unparsing.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRE_COMPILED_SCHEMA = new PropertyDescriptor.Builder()
            .name("pre-compiled-schema")
            .displayName("Pre-compiled Schema")
            .description("Specify whether the 'DFDL Schema File' property is a pre-compiled parser that can be reloaded or if it is a DFDL schema that needs to be compiled. Set to true if it is pre-compiled.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PLUGINS_AND_SCHEMAS = new PropertyDescriptor.Builder()
            .name("plugins-and-schemas")
            .displayName("Plugins and Schemas")
            .description("Comma-separated list of paths to directories, files, and jars to find Daffodil plugins or the 'Daffodil Schema File'")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .dynamicallyModifiesClasspath(true)
            .build();

    static final String XML_MIME_TYPE = "application/xml";
    static final String JSON_MIME_TYPE = "application/json";

    static final String XML_VALUE = "xml";
    static final String JSON_VALUE = "json";
    static final String ATTRIBUTE_VALUE = "use mime.type attribute";

    static final AllowableValue INFOSET_TYPE_XML = new AllowableValue(XML_VALUE, XML_VALUE, "The FlowFile representation is XML");
    static final AllowableValue INFOSET_TYPE_JSON = new AllowableValue(JSON_VALUE, JSON_VALUE, "The FlowFile representation is JSON");
    static final AllowableValue INFOSET_TYPE_ATTRIBUTE = new AllowableValue(ATTRIBUTE_VALUE, ATTRIBUTE_VALUE, "The FlowFile representation is determined based on the mime.type attribute");

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache Size")
            .description("Maximum number of compiled DFDL schemas to cache. Zero disables the cache. Restart the processor to manually empty the cache and recompile/reload schemas as needed.")
            .required(true)
            .defaultValue("50")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_LAST_ACCESS = new PropertyDescriptor.Builder()
            .name("cache-ttl-after-last-access")
            .displayName("Cache TTL After Last Access")
            .description("Defines how long keep unused compiled DFDL schemas in the cache before removing them to free memory. Defaults to '0 seconds' which means to never remove cached schemas. Restart the processor to manually empty the cache and recompile/reload schemas as needed.")
            .required(true)
            .defaultValue("0 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final String OFF_VALUE = "off";
    static final String LIMITED_VALUE = "limited";
    static final String FULL_VALUE = "full";

    static final AllowableValue VALIDATION_MODE_OFF = new AllowableValue(OFF_VALUE, OFF_VALUE, "Disable infoset validation");
    static final AllowableValue VALIDATION_MODE_LIMITED= new AllowableValue(LIMITED_VALUE, LIMITED_VALUE, "Facet/restriction validation using Daffodil");
    static final AllowableValue VALIDATION_MODE_FULL = new AllowableValue(FULL_VALUE, FULL_VALUE, "Full schema validation using Xerces");

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
            .name("validation-mode")
            .displayName("Validation Mode")
            .description("The type of validation to be performed on the infoset.")
            .required(true)
            .defaultValue(OFF_VALUE)
            .allowableValues(VALIDATION_MODE_OFF, VALIDATION_MODE_LIMITED, VALIDATION_MODE_FULL)
            .build();

    /**
     * This is not static like the other PropertyDescriptors. This is because
     * the allowable values differ based on whether this is parse or unparse
     * (mime.type attribute is not allow for parse). So on init() we will
     * create this property descriptor accordingly.
     */
    private PropertyDescriptor INFOSET_TYPE = null;


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("When a parse/unparse is successful, the resulting FlowFile is routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When a parse/unparse fails, it will be routed to this relationship")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<AllowableValue> allowableInfosetTypeValues = new ArrayList(Arrays.asList(INFOSET_TYPE_XML, INFOSET_TYPE_JSON));
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
            .allowableValues(allowableInfosetTypeValues.toArray(new AllowableValue[allowableInfosetTypeValues.size()]))
            .build();

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DFDL_SCHEMA_FILE);
        properties.add(PRE_COMPILED_SCHEMA);
        properties.add(INFOSET_TYPE);
        properties.add(VALIDATION_MODE);
        properties.add(CACHE_SIZE);
        properties.add(CACHE_TTL_AFTER_LAST_ACCESS);
        properties.add(PLUGINS_AND_SCHEMAS);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private LoadingCache<CompilationParams, DataProcessor> cache;

    /**
     * Stores all parameters needed to create a DataProcessor
     *
     * DataProcessors compiled with these parameters are stored in a Cache using the associated
     * CompilationParams as a key. Any values needed to configure DataProcessor creation or
     * modification should be added as constructor parameters and hashCode() and equals()
     * functions updated accordingly. This ensures value equality checks are performed correctly
     * in the Cache lookup.
     */
    static class CompilationParams {
        public String dfdlSchema;
        public Boolean preCompiled;
        public ValidationMode validationMode;

        public CompilationParams(
            String dfdlSchema,
            Boolean preCompiled,
            ValidationMode validationMode) {

            this.dfdlSchema = dfdlSchema;
            this.preCompiled = preCompiled;
            this.validationMode = validationMode;
        }

        public int hashCode() {
          return Objects.hash(dfdlSchema, preCompiled, validationMode);
        }

        public boolean equals(Object obj) {
          if (!(obj instanceof CompilationParams)) return false;
          if (obj == this) return true;

          CompilationParams that = (CompilationParams)obj;
          return Objects.equals(this.dfdlSchema, that.dfdlSchema) &&
                 Objects.equals(this.preCompiled, that.preCompiled) &&
                 Objects.equals(this.validationMode, that.validationMode);
        }

        /**
         * Creates a DataProcessor using only state in the compilation parameters
         *
         * This function must create and modify a DataProcessor using the provided parameters.
         * The resulting DataProcessor will be added to a Cache so that this function does not
         * need to be called again for the same set of parameters. This avoids compiling the
         * same schema multiple times.
         *
         * Note that changes to the DataProcessor (e.g. calls to with*() functions) should not
         * happen outside of this function, as those changes will not be cached and will need to
         * be done for every flow file, which could have performance implications.
         */
        DataProcessor newDataProcessor(ComponentLog logger) throws DaffodilCompileException {

            // Try to find the schema to compile or reload. If dfdlSchema is a file that exists,
            // we just use that. If dfdlSchema is not a file, try to find it on the classpath,
            // most likely in a jar or file found in the 'Plugins and Schemas' property. If the
            // schema is in a jar, this should be an absolute resource path (i.e. has a leading
            // slash). If the schema is not in a jar, then it should just be the file name
            // without a preceding slash. The latter works because of NiFis InstanceClassLoader,
            // which makes non-jar files available via getResource by just the file name.
            URL schemaURL = null;
            File f = new File(this.dfdlSchema);
            if (f.isFile()) {
                try {
                    schemaURL = f.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new DaffodilCompileException("Invalid 'DFDL Schema File' property: " + e);
                }
            } else {
                // it is important to use getClassLoader.getResource() here. If we just do
                // getClass.getResource() then Java will prepend the classes package to
                // dfdlSchema values that are not absolute, which breaks the ability for the
                // InstanceClassLoader to find non-jar resources
                schemaURL = getClass().getClassLoader().getResource(this.dfdlSchema);
            }

            if (schemaURL == null) {
                logger.error("Failed to find 'DFDL Schema File' property as a file or from 'Plugins and Schemas': " + this.dfdlSchema);
                throw new DaffodilCompileException("Failed to find 'DFDL Schema File' property as a file or in 'Plugins and Schemas': " + this.dfdlSchema);
            }

            Compiler c = Daffodil.compiler();
            DataProcessor dp;
            if (this.preCompiled) {
                try {
                    InputStream is = schemaURL.openStream();
                    ReadableByteChannel rbc = Channels.newChannel(is);
                    dp = c.reload(rbc);
                    rbc.close();
                    is.close();
                } catch (InvalidParserException|IOException e) {
                    logger.error("Failed to reload pre-compiled DFDL schema: " + this.dfdlSchema + ". " + e.getMessage());
                    throw new DaffodilCompileException("Failed to reload pre-compiled DFDL schema: " + this.dfdlSchema + ". " + e.getMessage());
                }
            } else {
                try {
                    ProcessorFactory pf = c.compileSource(schemaURL.toURI());
                    if (pf.isError()) {
                        logger.error("Failed to compile DFDL schema: " + this.dfdlSchema);
                        AbstractDaffodilProcessor.logDiagnostics(logger, pf);
                        throw new DaffodilCompileException("Failed to compile DFDL schema: " + this.dfdlSchema);
                    }
                    dp = pf.onPath("/");
                    if (dp.isError()) {
                        logger.error("Failed to compile DFDL schema: " + this.dfdlSchema);
                        AbstractDaffodilProcessor.logDiagnostics(logger, dp);
                        throw new DaffodilCompileException("Failed to compile DFDL schema: " + this.dfdlSchema);
                    }
                } catch (IOException e) {
                    throw new DaffodilCompileException("Failed to compile DFDL schema: " + this.dfdlSchema, e);
                } catch (URISyntaxException e) {
                    throw new AssertionError("invalid URI should no be possible: " + e);
                }
            }
            try {
                dp = dp.withValidationMode(this.validationMode);
            } catch (InvalidUsageException e) {
                throw new AssertionError("invalid usage of Daffodil API: " + e);
            }
            return dp;
        }
    }

    protected DataProcessor getDataProcessor(CompilationParams params) throws DaffodilCompileException {
        if (cache != null) {
            try {
                return cache.get(params);
            } catch (ExecutionException e) {
                throw new DaffodilCompileException(e);
            }
        } else {
            return params.newDataProcessor(getLogger());
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        // all dynamic properties are treated as variables, with the property name/value set as the variable
        // name/value. NiFi expressions are allowed to support dynamic variable values, such as from flow file
        // attributes. We do not need a validator, since a variable value could theoretically contain
        // anyting--we rely on Daffodil to check the variable value according to its variable type when we
        // provide the variables at parse/unparse time.
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .dynamic(true)
            .build();
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        final Integer cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(CACHE_TTL_AFTER_LAST_ACCESS).asTimePeriod(TimeUnit.SECONDS);

        if (cacheSize != null && cacheSize > 0) {
            CacheBuilder cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheTTL != null && cacheTTL > 0) {
                cacheBuilder = cacheBuilder.expireAfterAccess(cacheTTL, TimeUnit.SECONDS);
            }

            cache = cacheBuilder.build(
               new CacheLoader<CompilationParams, DataProcessor>() {
                   public DataProcessor load(CompilationParams params) throws DaffodilCompileException {
                       return params.newDataProcessor(logger);
                   }
               });
        } else {
            cache = null;
            logger.warn("Daffodil data processor cache disabled because cache size is set to 0.");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
        final String dfdlSchema = context.getProperty(DFDL_SCHEMA_FILE).evaluateAttributeExpressions(original).getValue();
        final Boolean preCompiled = context.getProperty(PRE_COMPILED_SCHEMA).evaluateAttributeExpressions(original).asBoolean();
        String infosetTypeValue = context.getProperty(INFOSET_TYPE).getValue();
        final String infosetType;
        final ValidationMode validationMode;

        switch (context.getProperty(VALIDATION_MODE).getValue()) {
            case OFF_VALUE: validationMode = ValidationMode.Off; break;
            case LIMITED_VALUE: validationMode = ValidationMode.Limited; break;
            case FULL_VALUE: validationMode = ValidationMode.Full; break;
            default: throw new AssertionError("validation mode was not one of 'off', 'limited', or 'full'");
        }

        CompilationParams params = new CompilationParams(dfdlSchema, preCompiled, validationMode);

        if (infosetTypeValue.equals(ATTRIBUTE_VALUE)) {
            if (!isUnparse()) { throw new AssertionError("infoset type 'attribute' should only occur with Daffodil unparse"); }

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
            // Get the DataProcessor, likely from a cache of already compiled data processors. The only change
            // that should happen to the cached DataProcessor is setting variables specific to the flowfile.
            // We don't cache DataProcessor with variables preset because variables are expressions and could
            // change per flow file. Also, assigning external variables is pretty efficient so not worth
            // caching.
            final DataProcessor cachedDP;
            final DataProcessor dpForProcessing;
            try {
                cachedDP = getDataProcessor(params);
            } catch (DaffodilCompileException e) {
                throw new ProcessException(e);
            }

            // Treat dynamic properties as variables. If the value of the variable is the empty
            // string (or an expression that evaluates to the empty string), the dynamic property is
            // ignored and is not added as a variable. This supports expressions that determine that
            // a variable does not apply to a schema and to ignore it, since all varibles passed to
            // withExternalVariables must be valid for that schema.
            final LinkedHashMap<String, String> variableMap = new LinkedHashMap<>();
            for (final PropertyDescriptor pd : context.getProperties().keySet()) {
                if (pd.isDynamic()) {
                    final String value = context.getProperty(pd.getName()).evaluateAttributeExpressions(original).getValue();
                    if (!value.isEmpty()) {
                        variableMap.put(pd.getName(), value);
                    }
                }
            }

            if (variableMap.isEmpty()) {
                dpForProcessing = cachedDP;
            } else {
                try {
                    dpForProcessing = cachedDP.withExternalVariables(variableMap);
                } catch (ExternalVariableException ex) {
                    throw new ProcessException("variables not valid for schema: " + ex.getMessage());
                }
            }

            FlowFile output = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {
                    // Parse or unparse the flow file, reading from the input stream and writing
                    // to the output stream
                    processWithDaffodil(dpForProcessing, original, in, out, infosetType);
                }
            });

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
            logger.error("Failed to process {} due to {}", new Object[]{original, e.getMessage()});
            session.transfer(original, REL_FAILURE);
        }
    }

    public static void logDiagnostics(ComponentLog logger, WithDiagnostics withDiags) {
        final List<Diagnostic> diags = withDiags.getDiagnostics();
        for (Diagnostic diag : diags) {
            String message = diag.toString();
            if (diag.isError()) {
                logger.error(message);
            } else {
                logger.warn(message);
            }
        }
    }
}
