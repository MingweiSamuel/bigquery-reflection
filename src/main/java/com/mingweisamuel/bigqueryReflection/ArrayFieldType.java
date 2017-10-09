package com.mingweisamuel.bigqueryReflection;

import com.google.cloud.bigquery.Field;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ArrayFieldType implements FieldType {

    private final Class<? extends Serializable> clazz;
    public ArrayFieldType() {
        this(null);
    }
    public ArrayFieldType(Class<? extends Serializable> clazz) {
        this.clazz = clazz;
    }

    private static PrimitiveFieldType getFieldType(Object o) {
        if (o instanceof Long)
            return PrimitiveFieldType.INTEGER;
        if (o instanceof Double)
            return PrimitiveFieldType.FLOAT;
        if (o instanceof String)
            return PrimitiveFieldType.STRING;
        if (o instanceof Boolean)
            return PrimitiveFieldType.BOOLEAN;
        throw new IllegalArgumentException("Unknown type: " + o.getClass().getName());
    }

    @Override
    public Object parse(Object value) {
        List<?> list = (List<?>) value;
        return list.stream().map((Function<Object, Object>) o -> {
            if (clazz == null)
                return getFieldType(o).parse(o);
            if (o instanceof Map) {
                //noinspection unchecked
                return BqReflection.of(clazz).parse((Map<String, Object>) o);
            }
            throw new IllegalStateException("Failed to parse array value: \"" + o + "\".");
        }).collect(Collectors.toList());
    }

    @Override
    public Object serialize(Object value) {
        throw new IllegalStateException("Serialization of array fields is not supported.");
//        List<?> list = (List<?>) value;
//        return list.stream().map((Function<Object, Object>) o -> {
//            if (clazz == null)
//                return getFieldType(o).serialize(o);
//            if (clazz.isInstance(o)) {
//                //noinspection unchecked
//                return BqReflection.of(clazz).serialize(clazz.cast(o));
//            }
//            throw new IllegalStateException("Failed to parse array value: \"" + o + "\".");
//        }).collect(Collectors.toList());
    }

    @Override
    public Field.Type getBqType() {
        throw new IllegalStateException("Serialization of array fields is not supported.");
    }

    @Override
    public void outputBytes(DataOutputStream output, Object value) throws IOException {
        throw new IllegalStateException("Serialization of array fields is not supported.");
    }
}
