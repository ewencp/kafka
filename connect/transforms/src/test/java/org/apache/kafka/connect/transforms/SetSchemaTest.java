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


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SetSchemaTest {
    private final SetSchema<SinkRecord> xform = new SetSchema.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void testInferredInt8() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = (byte) 1;
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_INT8_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredInt16() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = (short) 1;
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_INT16_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredInt32() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = 1;
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredInt64() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = (long) 1;
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredFloat32() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = 1.2f;
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_FLOAT32_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredFloat64() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = 1.2;
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_FLOAT64_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredBoolean() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = true;
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_BOOLEAN_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredString() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = "example";
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredBytes() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = "example".getBytes();
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredBytesByteBuffer() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = ByteBuffer.wrap("example".getBytes());
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, updatedRecord.valueSchema());
        assertTrue(value == updatedRecord.value());
    }

    @Test
    public void testInferredArray() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = Arrays.asList(null, "foo", "bar", "baz", null); // null value validates optional type
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));
        final Schema expectedSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
        assertEquals(expectedSchema, updatedRecord.valueSchema());
        assertEquals(value, updatedRecord.value());
    }

    @Test(expected = DataException.class)
    public void testInferredArrayEmpty() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = new ArrayList();
        xform.apply(createSchemalessRecord(value));
    }

    @Test(expected = DataException.class)
    public void testInferredArrayMismatchingElements() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Object value = Arrays.asList("foo", 12);
        xform.apply(createSchemalessRecord(value));
    }

    @Test
    public void testInferredMap() {
        xform.configure(Collections.singletonMap("infer", "true"));
        // See test below for why we use LinkedHashMap here
        final Map<String, Object> value = new LinkedHashMap<>();
        value.put("foo", "bar");
        value.put("baz", 16);
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));

        // Should be in alphabetical order
        final Schema expectedSchema = SchemaBuilder.struct().optional()
                .field("baz", Schema.OPTIONAL_INT32_SCHEMA)
                .field("foo", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        assertEquals(expectedSchema, updatedRecord.valueSchema());
        final Struct expectedValue = new Struct(expectedSchema)
                .put("baz", 16)
                .put("foo", "bar");
        assertEquals(expectedValue, updatedRecord.value());
    }

    @Test
    public void testInferredMapOrdering() {
        // Test the same map, but with different iteration order to ensure that the same structure always results in the
        // same schema. This is necessary because schemaless data is only guaranteed to have a Map which could have
        // arbitrary ordering
        xform.configure(Collections.singletonMap("infer", "true"));
        final Map<String, Object> value = new LinkedHashMap<>();
        value.put("baz", 16);
        value.put("foo", "bar");
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));

        final Schema expectedSchema = SchemaBuilder.struct().optional()
                .field("baz", Schema.OPTIONAL_INT32_SCHEMA)
                .field("foo", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        assertEquals(expectedSchema, updatedRecord.valueSchema());
        final Struct expectedValue = new Struct(expectedSchema)
                .put("baz", 16)
                .put("foo", "bar");
        assertEquals(expectedValue, updatedRecord.value());
    }

    @Test(expected = DataException.class)
    public void testInferredMapNullKey() {
        xform.configure(Collections.singletonMap("infer", "true"));
        // null keys are invalid for structs
        xform.apply(createSchemalessRecord(Collections.singletonMap(null, "foo")));
    }

    @Test(expected = DataException.class)
    public void testInferredMapNullValue() {
        xform.configure(Collections.singletonMap("infer", "true"));
        // cannot infer a schema if there's only a single null value
        xform.apply(createSchemalessRecord(Collections.singletonMap("foo", null)));
    }

    @Test(expected = DataException.class)
    public void testInferredMapMismatchingKeys() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Map<Object, String> value = new HashMap<>();
        value.put("foo", "bar");
        value.put(12, "raz");
        xform.apply(createSchemalessRecord(value));
    }

    @Test
    public void testNestedStructure() {
        xform.configure(Collections.singletonMap("infer", "true"));
        final Map<String, Object> value = new LinkedHashMap<>();
        final Map<String, Object> substructure = new LinkedHashMap<>();
        substructure.put("bar", "baz");
        value.put("foo", substructure);
        final SinkRecord updatedRecord = xform.apply(createSchemalessRecord(value));

        final Schema expectedSubstructureSchema = SchemaBuilder.struct().optional()
                .field("bar", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Schema expectedSchema = SchemaBuilder.struct().optional()
                .field("foo", expectedSubstructureSchema)
                .build();
        assertEquals(expectedSchema, updatedRecord.valueSchema());
        final Struct expectedValue = new Struct(expectedSchema)
                .put("foo", new Struct(expectedSubstructureSchema).put("bar", "baz"));
        assertEquals(expectedValue, updatedRecord.value());
    }

    private SinkRecord createSchemalessRecord(Object value) {
        return new SinkRecord("topic", 0, null, null, null, value, 0);
    }
}
