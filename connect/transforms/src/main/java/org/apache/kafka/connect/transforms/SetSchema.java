/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireNoSchema;

public abstract class SetSchema<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Apply a schema to schemaless data, either using a predefined schema against which the data is validated " +
                    "or by inferring a schema based on the data. " +
                    "Schemas can be applied to the key (<code>" + Key.class.getName() + "</code>)"
                    + " or value (<code>" + Value.class.getName() + "</code>). " +
                    "When using inferred schemas, combine with the SetSchemaMetadata transformation if you also need " +
                    "to set the name or version. Inferred schemas are always optional and use optional schemas for " +
                    "Struct fields, array elements, and map elements.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    }

    @Override
    public R apply(R record) {
        final Schema originalSchema = operatingSchema(record);
        requireNoSchema(originalSchema, "setting or inferring the schema");
        final SchemaAndValue updated = inferSchema(operatingValue(record));
        return newRecord(record, updated.schema(), updated.value());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updated);

    /**
     * Set the schema name, version or both on the record's key schema.
     */
    public static class Key<R extends ConnectRecord<R>> extends SetSchema<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updated) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updated, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    /**
     * Set the schema name, version or both on the record's value schema.
     */
    public static class Value<R extends ConnectRecord<R>> extends SetSchema<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updated) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updated, record.timestamp());
        }
    }

    private SchemaAndValue inferSchema(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Byte) {
            return new SchemaAndValue(Schema.OPTIONAL_INT8_SCHEMA, value);
        } else if (value instanceof Short) {
            return new SchemaAndValue(Schema.OPTIONAL_INT16_SCHEMA, value);
        } else if (value instanceof Integer) {
            return new SchemaAndValue(Schema.OPTIONAL_INT32_SCHEMA, value);
        } else if (value instanceof Long) {
            return new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, value);
        } else if (value instanceof Float) {
            return new SchemaAndValue(Schema.OPTIONAL_FLOAT32_SCHEMA, value);
        } else if (value instanceof Double) {
            return new SchemaAndValue(Schema.OPTIONAL_FLOAT64_SCHEMA, value);
        } else if (value instanceof Boolean) {
            return new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, value);
        } else if (value instanceof String) {
            return new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, value);
        } else if (value instanceof byte[]) {
            return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
        } else if (value instanceof ByteBuffer) {
            return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
        } else if (value instanceof Collection) {
            Collection<Object> arrayValue = (Collection<Object>) value;
            if (arrayValue.isEmpty()) {
                throw new DataException("Cannot infer schema from an empty array/list type");
            }
            final List<Object> convertedValues = new ArrayList();
            Schema elementSchema = null;
            for (Object element : arrayValue) {
                SchemaAndValue inferred = inferSchema(element);
                if (element == null) {
                    // This is ok, just skip over any conversion/checks and just add the null value to the list
                    convertedValues.add(null);
                    continue;
                } else if (elementSchema == null) {
                    elementSchema = inferred.schema();
                } else if (!elementSchema.equals(inferred.schema())) {
                    throw new DataException("All inferred schemas in an array/list must be equal: " + elementSchema + " != " + inferred.schema());
                }
                convertedValues.add(inferred.value());
            }
            if (elementSchema == null) {
                throw new DataException("Cannot infer schema of array type if no elements have non-null values");
            }
            return new SchemaAndValue(SchemaBuilder.array(elementSchema).optional().build(), convertedValues);
        } else if (value instanceof Map) {
            // Schemaless maps convert to Structs with schemas
            Map<Object, Object> mapValue = (Map<Object, Object>) value;
            if (mapValue.isEmpty()) {
                throw new DataException("Cannot infer schema from an empty map type");
            }

            final Object[] keys = mapValue.keySet().toArray();
            try {
                Arrays.sort(keys);
            } catch (ClassCastException e) {
                throw new DataException("Cannot infer schema with mismatching keys", e);
            }

            // Pass 1 to build the schema
            SchemaBuilder builder = SchemaBuilder.struct().optional();
            // Temporary holder for converted values
            Map<String, Object> convertedFields = new HashMap();
            for (Object key : Arrays.asList(keys)) {
                if (key == null) {
                    throw new DataException("Map keys may not be null, they must be valid strings");
                }
                SchemaAndValue keyConverted = inferSchema(key);
                if (!Schema.OPTIONAL_STRING_SCHEMA.equals(keyConverted.schema())) {
                    throw new DataException("Inferred schemas must have string keys, found " + keyConverted);
                }

                Object fieldValue = mapValue.get(key);
                if (fieldValue == null) {
                    throw new DataException("Map values may not be null");
                }
                SchemaAndValue valueConverted = inferSchema(fieldValue);
                builder.field((String) keyConverted.value(), valueConverted.schema());
                convertedFields.put((String) key, valueConverted.value());
            }

            Schema schema = builder.build();

            // Pass 2 to build the value
            Struct result = new Struct(schema);
            for (Map.Entry<String, Object> entry : convertedFields.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }

            return new SchemaAndValue(schema, result);
        }

        throw new DataException("Unexpected data type for SetSchema: " + value.getClass().getName());
    }
}
