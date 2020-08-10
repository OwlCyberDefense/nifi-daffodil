package com.tresys.nifi.controllers;

import com.tresys.nifi.util.DaffodilResources.DataProcessorSchemaPair;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Tags({"xml", "json", "daffodil", "dfdl", "schema", "xsd"})
@CapabilityDescription("Use Daffodil and a user-specified DFDL schema to transform data to an infoset into Records")
public class DaffodilReader extends AbstractDaffodilController implements RecordReaderFactory {

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream inputStream,
                                           long inputLength, ComponentLog logger) {
        try {
            DataProcessorSchemaPair pair = getPair(variables);
            return new DaffodilRecordReader(pair.getSchema(), inputStream, pair.getDataProcessor(), streamMode, logger);
        } catch (IOException ioe) {
            getLogger().error("Unable to obtain DataProcessor and/or Schema due to {}", new Object[]{ioe.getMessage()});
            throw new ProcessException("Unable to obtain DataProcessor and/or Schema", ioe);
        }
    }

}
