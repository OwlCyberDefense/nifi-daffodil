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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.util.StopWatch;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import edu.illinois.ncsa.daffodil.japi.Compiler;
import edu.illinois.ncsa.daffodil.japi.Daffodil;
import edu.illinois.ncsa.daffodil.japi.DataProcessor;
import edu.illinois.ncsa.daffodil.japi.Diagnostic;
import edu.illinois.ncsa.daffodil.japi.ProcessorFactory;
import edu.illinois.ncsa.daffodil.japi.WithDiagnostics;


public abstract class AbstractDaffodilProcessor extends AbstractProcessor {

    abstract protected void processWithDaffodil(final DataProcessor dp, final FlowFile ff, final InputStream in, final OutputStream out) throws IOException;

    public static final PropertyDescriptor DFDL_SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("dfdl-schema-file")
            .displayName("DFDL Schema File")
            .description("Full path to the DFDL schema file that is to be used for parsing/unparsing.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache Size")
            .description("Maximum number of compiled DFDL schemas to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("50")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_LAST_ACCESS = new PropertyDescriptor.Builder()
            .name("cache-ttl-after-last-access")
            .displayName("Cache TTL After Last Access")
            .description("The cache TTL (time-to-live) or how long to keep compiled DFDL schemas in the cache after last access.")
            .required(true)
            .defaultValue("30 mins")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

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
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DFDL_SCHEMA_FILE);
        properties.add(CACHE_SIZE);
        properties.add(CACHE_TTL_AFTER_LAST_ACCESS);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private LoadingCache<String, DataProcessor> cache;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    private DataProcessor newDaffodilDataProcessor(String dfdlSchema) throws IOException {
        File f = new File(dfdlSchema);
        Compiler c = Daffodil.compiler();
        ProcessorFactory pf = c.compileFile(f);
        if (pf.isError()) {
            getLogger().error("Failed to compile DFDL schema: " + dfdlSchema);
            logDiagnostics(pf);
            throw new DaffodilCompileException("Failed to compile DFDL schema: " + dfdlSchema);
        }
        DataProcessor dp = pf.onPath("/");
        if (dp.isError()) {
            getLogger().error("Failed to compile DFDL schema: " + dfdlSchema);
            logDiagnostics(dp);
            throw new DaffodilCompileException("Failed to compile DFDL schema: " + dfdlSchema);
        }
        return dp;
    }

    protected DataProcessor getDataProcessor(String dfdlSchema) throws IOException {
        if (cache != null) {
            try {
                return cache.get(dfdlSchema);
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        } else {
            return newDaffodilDataProcessor(dfdlSchema);
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        final Integer cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(CACHE_TTL_AFTER_LAST_ACCESS).asTimePeriod(TimeUnit.SECONDS);

        if (cacheSize > 0) {
            CacheBuilder cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheTTL > 0) {
                cacheBuilder = cacheBuilder.expireAfterAccess(cacheTTL, TimeUnit.SECONDS);
            }

            cache = cacheBuilder.build(
               new CacheLoader<String, DataProcessor>() {
                   public DataProcessor load(String dfdlSchema) throws IOException {
                       return newDaffodilDataProcessor(dfdlSchema);
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
        final String dfdlSchema = context.getProperty(DFDL_SCHEMA_FILE)
            .evaluateAttributeExpressions(original)
            .getValue();

        try {
            FlowFile output = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {
                    DataProcessor dp = getDataProcessor(dfdlSchema);
                    processWithDaffodil(dp, original, in, out);
                }
            });
            session.transfer(output, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(output, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.debug("Processed {}", new Object[]{original});
        } catch (ProcessException e) {
            logger.error("Failed to process {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        }
    }

    protected void logDiagnostics(WithDiagnostics withDiags) {
        final ComponentLog logger = getLogger();
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
