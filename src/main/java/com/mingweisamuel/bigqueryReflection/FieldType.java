package com.mingweisamuel.bigqueryReflection;

import com.google.cloud.bigquery.Field;

import java.io.DataOutputStream;
import java.io.IOException;

public interface FieldType {
    Object parse(Object value);
    Object serialize(Object value);
    Field.Type getBqType();
    void outputBytes(DataOutputStream output, Object value) throws IOException;
}
