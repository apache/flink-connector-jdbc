package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcOutputSerializerTest {

    @Test
    void testSerializer() {
        TypeInformation<Row> typeInformation = TypeInformation.of(Row.class);
        TypeSerializer<Row> typeSerializer =
                typeInformation.createSerializer(new ExecutionConfig());
        JdbcOutputSerializer<Row> serializer = JdbcOutputSerializer.of(typeSerializer);

        Row original = Row.of(123);
        Row noReuse = serializer.withObjectReuseEnabled(false).serialize(original);
        Row withReuse = serializer.withObjectReuseEnabled(true).serialize(original);

        assertThat(noReuse).isEqualTo(original);
        assertThat(withReuse).isEqualTo(original);

        original.setField(0, 321);

        // if disable object is reusable
        assertThat(noReuse).isEqualTo(original);
        // if enabled object is duplicate
        assertThat(withReuse).isNotEqualTo(original);
    }
}
