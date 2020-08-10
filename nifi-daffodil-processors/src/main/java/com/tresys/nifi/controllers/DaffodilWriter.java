package com.tresys.nifi.controllers;

import com.tresys.nifi.util.DaffodilCompileException;
import com.tresys.nifi.util.DaffodilResources.DataProcessorSchemaPair;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.OutputStream;
import java.util.Map;

@Tags({"xml", "json", "daffodil", "dfdl", "schema", "xsd"})
@CapabilityDescription("Use Daffodil and a user-specified DFDL schema to unparse a Daffodil Infoset into data")
public class DaffodilWriter extends AbstractDaffodilController implements RecordSetWriterFactory {

    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) {
        return readSchema;
    }

    @Override
    public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema,
                                        OutputStream out, Map<String, String> variables) {
        try {
            DataProcessorSchemaPair pair = getPair(variables);
            return new DaffodilRecordSetWriter(pair.getDataProcessor(), out, pair.getSchema(), streamMode, logger);
        } catch (DaffodilCompileException exception) {
            getLogger().error("Unable to obtain DataProcessor and/or Schema due to {}", new Object[]{exception.getMessage()});
            throw new ProcessException("Unable to obtain DataProcessor and/or Schema", exception);
        }
    }

}
