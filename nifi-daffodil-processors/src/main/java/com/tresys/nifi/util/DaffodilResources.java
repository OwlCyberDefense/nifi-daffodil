package com.tresys.nifi.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tresys.nifi.schema.RecordWalker;
import org.apache.daffodil.japi.Compiler;
import org.apache.daffodil.japi.Daffodil;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.Diagnostic;
import org.apache.daffodil.japi.InvalidUsageException;
import org.apache.daffodil.japi.ProcessorFactory;
import org.apache.daffodil.japi.ValidationMode;
import org.apache.daffodil.japi.WithDiagnostics;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.RecordSchema;
import scala.Predef;
import scala.collection.JavaConverters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class holds a variety of Properties and static methods that are used by both the
 * Daffodil Controllers and Processors.  Many of these interact with a Cache that holds
 * DataProcessor objects so that a given configuration with a Schema need not be re-compiled
 * every time a Parse/Unparse is performed.
 */
public class DaffodilResources {

    public static final PropertyDescriptor DFDL_SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("dfdl-schema-file")
            .displayName("DFDL Schema File")
            .description("Full path to the DFDL schema file that is to be used for parsing/unparsing.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    public static final String OFF_VALUE = "off";
    public static final String LIMITED_VALUE = "limited";
    public static final String FULL_VALUE = "full";

    public static final AllowableValue VALIDATION_MODE_OFF
            = new AllowableValue(OFF_VALUE, OFF_VALUE, "Disable infoset validation");
    public static final AllowableValue VALIDATION_MODE_LIMITED
            = new AllowableValue(LIMITED_VALUE, LIMITED_VALUE, "Facet/restriction validation using Daffodil");
    public static final AllowableValue VALIDATION_MODE_FULL
            = new AllowableValue(FULL_VALUE, FULL_VALUE, "Full schema validation using Xerces");

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
            .name("validation-mode")
            .displayName("Validation Mode")
            .description("The type of validation to be performed on the infoset.")
            .required(true)
            .defaultValue(OFF_VALUE)
            .allowableValues(VALIDATION_MODE_OFF, VALIDATION_MODE_LIMITED, VALIDATION_MODE_FULL)
            .build();

    /**
     * Currently only external variables are supported as configuration file items.
     */
    public static final PropertyDescriptor CONFIG_FILE = new PropertyDescriptor.Builder()
            .name("config_file")
            .displayName("DFDL Config File Path")
            .description("Path to an XML-based DFDL Configuration file that contains a list of external variables and tunables.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName == null || propertyDescriptorName.replaceAll("\\s+", "").isEmpty()) {
            return null;
        } else {
            boolean isTunable = propertyDescriptorName.startsWith("+");
            String displayName = isTunable ? "Tunable " + propertyDescriptorName.substring(1) :
                    "External Variable " + propertyDescriptorName;
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(displayName)
                    .description("Value to configure a specific " + (isTunable ? "tunable" : "external variable") + " when parsing or unparsing")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();
        }
    }

    public static final List<PropertyDescriptor> daffodilProperties;
    public static final List<String> propertyNames;

    static {
        final List<PropertyDescriptor> propertyList = new ArrayList<>();
        propertyList.add(DaffodilResources.DFDL_SCHEMA_FILE);
        propertyList.add(DaffodilResources.VALIDATION_MODE);
        propertyList.add(DaffodilResources.CACHE_SIZE);
        propertyList.add(DaffodilResources.CACHE_TTL_AFTER_LAST_ACCESS);
        propertyList.add(DaffodilResources.CONFIG_FILE);
        daffodilProperties = Collections.unmodifiableList(propertyList);
        propertyNames = daffodilProperties.stream().map(PropertyDescriptor::getName).collect(Collectors.toList());
    }

    /**
     * Converts a Java Map to a Scala Map.  This method is needed to call the withExternalVariables method,
     * which is in the Daffodil Java API yet takes Scala objects as parameters.
     * @param javaMap a Java Map
     * @return the Java Map converted to a Scala Map
     */
    public static scala.collection.immutable.Map<String, String> hashMapToScalaMap(Map<String, String> javaMap) {
        return JavaConverters.mapAsScalaMapConverter(javaMap).asScala().toMap(Predef.$conforms());
    }

    public static void logDiagnostics(ComponentLog logger, WithDiagnostics diagnostics) {
        final List<Diagnostic> diags = diagnostics.getDiagnostics();
        for (Diagnostic diag : diags) {
            String message = diag.toString();
            if (diag.isError()) {
                logger.error(message);
            } else {
                logger.warn(message);
            }
        }
    }

    /**
     * Gets a NiFi RecordSchema from a passed in ProcessorFactory object using the Daffodil DSOM API
     * and a custom RecordWalker implementation of that API
     * @param pf the given ProcessorFactory object
     * @return a RecordSchema corresponding to pf
     * @throws DaffodilCompileException if the passed in ProcessorFactory is in an errored state
     */
    public static RecordSchema getSchema(ProcessorFactory pf) throws DaffodilCompileException {
        if (pf.isError()) {
            throw new DaffodilCompileException(pf.getDiagnostics().toString());
        }
        RecordWalker walker = new RecordWalker();
        walker.walkFromRoot(pf.experimental().rootView());
        return walker.result();
    }

    /**
     * Returns a Pair containing a RecordSchema and a Daffodil DataProcessor, configured
     * based on all of the supplied parameters.
     * @return a DataProcessorSchemaPair object as described above
     * @throws DaffodilCompileException if:
     *                      - the schema file fails to compile
     *                      - one of the properties is not successfully set
     *                      - there is a problem generating a DataProcessor from the ProcessorFactory
     */
    public static DataProcessorSchemaPair newDataProcessorSchemaPair(ComponentLog logger, String dfdlSchema,
                                                           String validationModeAsString,
                                                           HashMap<String, String> extVariableMap,
                                                           HashMap<String, String> tunableMap,
                                                           String configFile) throws DaffodilCompileException {
        File f = new File(dfdlSchema);
        Compiler daffodilCompiler = Daffodil.compiler();

        if (!tunableMap.keySet().isEmpty()) {
            daffodilCompiler = daffodilCompiler.withTunables(tunableMap);
        }

        ProcessorFactory pf;
        try {
            pf = daffodilCompiler.compileFile(f);
        } catch (IOException ioe) {
            throw new DaffodilCompileException("Failed to compile DFDL schema: " + dfdlSchema, ioe);
        }

        if (pf.isError()) {
            logger.error("Failed to compile DFDL schema: " + dfdlSchema);
            logDiagnostics(logger, pf);
            throw new DaffodilCompileException("Failed to compile DFDL schema: " + dfdlSchema);
        }
        RecordSchema schema = getSchema(pf);

        ValidationMode validationMode;
        switch (validationModeAsString) {
            case DaffodilResources.OFF_VALUE:
                validationMode = ValidationMode.Off;
                break;
            case DaffodilResources.LIMITED_VALUE:
                validationMode = ValidationMode.Limited;
                break;
            case DaffodilResources.FULL_VALUE:
                validationMode = ValidationMode.Full;
                break;
            default: throw new AssertionError("validation mode was not one of 'off', 'limited', or 'full'");
        }

        try {
            DataProcessor dp = pf.onPath("/");
            if (dp.isError()) {
                throw new DaffodilCompileException("DataProcessor error: " + dp.getDiagnostics().toString());
            }
            try {
                dp = dp.withValidationMode(validationMode);
                if (!extVariableMap.isEmpty()) {
                    dp = dp.withExternalVariables(hashMapToScalaMap(extVariableMap));
                }
                if (configFile != null && !configFile.replaceAll("\\s", "").isEmpty()) {
                    dp = dp.withExternalVariables(new File(configFile));
                }
                if (dp.isError()) {
                    throw new DaffodilCompileException("DataProcessor error: " + dp.getDiagnostics().toString());
                }
                return new DataProcessorSchemaPair(dp, schema);
            } catch (InvalidUsageException e) {
                throw new DaffodilCompileException(
                    "DataProcessor error when setting validation mode, ext. variables, or config file", e
                );
            }
        } catch (ProcessException e) {
            logger.error("Failed to process due to {}", new Object[]{e});
            throw new DaffodilCompileException("DataProcessor error due to ProcessException", e);
        }
    }

    /**
     * @param context ProcessContext from which various configuration items will be obtained
     * @param dfdlSchema path to a DFDL schema file
     * @return an existing ProcessorFactory object if it was already generated and cached, otherwise a new ProcessorFactory object
     * @throws DaffodilCompileException if creating a new ProcessorFactory object fails
     * or a PF expected to be cached is not successfully obtained.
     */
    public static DataProcessorSchemaPair getDataProcessorSchemaPair(ComponentLog logger, PropertyContext context,
                                                                     String dfdlSchema) throws DaffodilCompileException {
        final String validationMode = context.getProperty(DaffodilResources.VALIDATION_MODE).getValue();
        final String configFile = context.getProperty(DaffodilResources.CONFIG_FILE).getValue();
        HashMap<String, String> extVariableMap = new HashMap<>();
        HashMap<String, String> tunableMap = new HashMap<>();
        context.getAllProperties().forEach(
            (name, value) -> {
                // we have to handle these 2 cases explicitly because Infoset Type and StreamMode are not
                // shared, general properties; only the Processors have Infoset Type, and only the Controllers
                // have Stream Mode
                if (!propertyNames.contains(name) && !"infoset-type".equals(name) && !"stream-mode".equals(name)) {
                    boolean isTunable = name.startsWith("+");
                    // remove the + sign from the tunable variable key
                    String actualName = isTunable ? name.substring(1) : name;
                    if (isTunable) {
                        tunableMap.put(actualName, value);
                    } else {
                        extVariableMap.put(actualName, value);
                    }
                }
            }
        );
        if (cache != null) {
            try {
                return cache.get(new CacheKey(
                    dfdlSchema, validationMode, extVariableMap, tunableMap, configFile)
                );
            } catch (ExecutionException e) {
                throw new DaffodilCompileException("Could not obtain Processor from cache", e);
            }
        } else {
            return newDataProcessorSchemaPair(logger, dfdlSchema, validationMode, extVariableMap, tunableMap, configFile);
        }
    }

    public static LoadingCache<CacheKey, DataProcessorSchemaPair> cache;

    public static class CacheKey {
        private final String dfdlSchema;
        private final String validationMode;
        private final HashMap<String, String> externalVariables;
        private final HashMap<String, String> tunableMap;
        private final String configFile;

        public CacheKey(String dfdlSchema, String validationMode, HashMap<String, String> externalVariables,
                        HashMap<String, String> tunableMap, String configFile) {
            this.dfdlSchema = dfdlSchema;
            this.validationMode = validationMode;
            this.externalVariables = externalVariables;
            this.tunableMap = tunableMap;
            this.configFile = configFile;
        }

        public boolean equals(Object other) {
            if (other == this) return true;
            if (!(other instanceof CacheKey)) return false;
            CacheKey otherKey = (CacheKey) other;
            return Objects.equals(dfdlSchema, otherKey.dfdlSchema) &&
                Objects.equals(validationMode, otherKey.validationMode) &&
                Objects.equals(externalVariables, otherKey.externalVariables) &&
                Objects.equals(tunableMap, otherKey.tunableMap) &&
                Objects.equals(configFile, otherKey.configFile);
        }

        public int hashCode() {
            return Objects.hash(dfdlSchema, validationMode, externalVariables, tunableMap, configFile);
        }

    }

    public static class DataProcessorSchemaPair {
        private final DataProcessor dataProcessor;
        private final RecordSchema schema;

        public DataProcessorSchemaPair(DataProcessor dataProcessor, RecordSchema schema) {
            this.dataProcessor = dataProcessor;
            this.schema = schema;
        }

        public DataProcessor getDataProcessor() {
            return dataProcessor;
        }

        public RecordSchema getSchema() {
            return schema;
        }
    }

    public static void buildCache(ComponentLog logger, ProcessContext context) {
        final Integer cacheSize = context.getProperty(DaffodilResources.CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(DaffodilResources.CACHE_TTL_AFTER_LAST_ACCESS).asTimePeriod(TimeUnit.SECONDS);

        // Update the `cache` variable to contain a new DataProcessor/RecordSchema pair, where the key
        // is a CacheKey based on all the possible configuration items
        if (cacheSize > 0) {
            CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheTTL > 0) {
                cacheBuilder.expireAfterAccess(cacheTTL, TimeUnit.SECONDS);
            }
            cache = cacheBuilder.build(
                new CacheLoader<CacheKey, DataProcessorSchemaPair>() {
                    public DataProcessorSchemaPair load(CacheKey cacheKey) throws DaffodilCompileException {
                        return newDataProcessorSchemaPair(
                            logger, cacheKey.dfdlSchema, cacheKey.validationMode, cacheKey.externalVariables,
                            cacheKey.tunableMap, cacheKey.configFile
                        );
                    }
                }
            );
        } else {
            cache = null;
            logger.warn("Daffodil data processor cache disabled because cache size is set to 0.");
        }
    }

}
