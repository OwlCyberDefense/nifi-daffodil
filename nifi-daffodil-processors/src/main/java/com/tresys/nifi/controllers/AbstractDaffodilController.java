package com.tresys.nifi.controllers;

import com.tresys.nifi.util.DaffodilCompileException;
import com.tresys.nifi.util.DaffodilResources;
import static com.tresys.nifi.util.DaffodilResources.DataProcessorSchemaPair;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class AbstractDaffodilController extends AbstractControllerService {

    private volatile ConfigurationContext context;
    protected volatile StreamMode streamMode;

    @OnEnabled
    public void storeConfiguration(final ConfigurationContext context) {
        this.streamMode = StreamMode.valueOf(context.getProperty(STREAM_MODE).getValue());
        this.context = context;
    }

    protected DataProcessorSchemaPair getPair(Map<String, String> attributes) throws DaffodilCompileException {
        if (context == null) {
            return null;
        } else {
            String dfdlFile = context.getProperty(DaffodilResources.DFDL_SCHEMA_FILE).evaluateAttributeExpressions(attributes).getValue();
            return DaffodilResources.getDataProcessorSchemaPair(getLogger(), context, dfdlFile);
        }
    }

    public enum StreamMode {
        OFF, ALL_SUCCESSFUL, ONLY_SUCCESSFUL
    }

    public static final AllowableValue STREAM_MODE_OFF
            = new AllowableValue(StreamMode.OFF.name(), StreamMode.OFF.name(), "Stream Mode is off.");
    public static final AllowableValue STREAM_MODE_ALL_SUCCESSFUL
            = new AllowableValue(StreamMode.ALL_SUCCESSFUL.name(), StreamMode.ALL_SUCCESSFUL.name(),
            "Multiple records are parsed until there is no remaining data. If there is a failure, stop and discard all records.");
    public static final AllowableValue STREAM_MODE_ONLY_SUCCESSFUL
            = new AllowableValue(StreamMode.ONLY_SUCCESSFUL.name(), StreamMode.ONLY_SUCCESSFUL.name(),
            "Multiple records are parsed until there is no remaining data. If there is a failure, stop, and keep all successful records.");

    /**
     * If this Property is set to true, then multiple Records will be produced when there is leftover data, with each one beginning where
     * the last one left off.  Normally leftover data just errors out.  We will still route to failure if *any* of these Records
     * are not successfully produced.  Making this option true does not cause any issues for unparsing, as the unparse Record component is
     * a RecordSetWriterFactory, which is able to handle the data containing a set of Records rather than just one Record.
     */
    static final PropertyDescriptor STREAM_MODE = new PropertyDescriptor.Builder()
            .name("stream-mode")
            .displayName("Stream Mode")
            .description("Rather than throwing an error when left over data exists after a parse, one can repeat the parse with the remaining data. "
                    + "With the 'All Successful' Mode, an error is thrown if any of the parses fail, whereas with 'Only Successful', the parse will succeed,"
                    + " and only successful parses show up in the output.")
            .required(false)
            .defaultValue(STREAM_MODE_OFF.getValue())
            .allowableValues(STREAM_MODE_OFF, STREAM_MODE_ALL_SUCCESSFUL, STREAM_MODE_ONLY_SUCCESSFUL)
            .build();

    private static final List<PropertyDescriptor> controllerProperties;

    static {
        List<PropertyDescriptor> properties = new ArrayList<>(DaffodilResources.daffodilProperties);
        properties.add(STREAM_MODE);
        controllerProperties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return controllerProperties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return DaffodilResources.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

}