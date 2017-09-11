package win.pickban.maokai;

import com.google.cloud.bigquery.Field;

import java.io.DataOutputStream;
import java.io.IOException;

public enum PrimitiveFieldType implements FieldType {
    STRING {
        @Override
        public String parse(Object value) {
            return value.toString();
        }
        @Override
        public Field.Type getBqType() {
            return Field.Type.string();
        }
        @Override
        public void outputBytes(DataOutputStream output, Object value) throws IOException {
            output.writeUTF((String) value);
        }
    },
    INTEGER {
        @Override
        public Long parse(Object value) {
            return value == null ? 0 : Long.parseLong(value.toString());
        }
        @Override
        public Field.Type getBqType() {
            return Field.Type.integer();
        }
        @Override
        public void outputBytes(DataOutputStream output, Object value) throws IOException {
            output.writeLong((Long) value);
        }
    },
    FLOAT {
        @Override
        public Double parse(Object value) {
            return value == null ? 0 : Double.parseDouble(value.toString());
        }
        @Override
        public Field.Type getBqType() {
            return Field.Type.floatingPoint();
        }
        @Override
        public void outputBytes(DataOutputStream output, Object value) throws IOException {
            output.writeDouble((Double) value);
        }
    },
    BOOLEAN {
        @Override
        public Boolean parse(Object value) {
            return Boolean.parseBoolean(value.toString());
        }
        @Override
        public Field.Type getBqType() {
            return Field.Type.bool();
        }
        @Override
        public void outputBytes(DataOutputStream output, Object value) throws IOException {
            output.writeBoolean((Boolean) value);
        }
    },
    TIMESTAMP {
        @Override
        public Long parse(Object value) {
            return TimestampUtils.parseBqTimestamp(value.toString());
        }
        @Override
        public Object serialize(Object value) {
            return value == null ? null : ((Long) value) / 1000.0;
        }
        @Override
        public Field.Type getBqType() {
            return Field.Type.timestamp();
        }
        @Override
        public void outputBytes(DataOutputStream output, Object value) throws IOException {
            INTEGER.outputBytes(output, value);
        }
    };
    public Object serialize(Object value) {
        return value;
    }
}
