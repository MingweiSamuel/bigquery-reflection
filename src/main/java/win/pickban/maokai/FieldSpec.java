package win.pickban.maokai;

import com.google.api.services.bigquery.model.TableRow;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;

class FieldSpec {

    private final boolean isPartition;
    private final boolean isExcluded;
    private final boolean isKey;
    private final String computedExpression;
    private final boolean isAutoTimestamp;

    private final Integer orderNumber;
    private final boolean orderAsc;

    private final Field field;
    private final String columnName;
    private final FieldType type;

    private final String description;
    private final boolean descriptionTriggerUpdate;

    /**
     * Gets the FieldSpec for the field.
     * @return The FieldSpec of null if the field isn't a Bq field.
     */
    static FieldSpec get(Field field) {
        BqField bqf = field.getAnnotation(BqField.class);
        BqFieldPartition bqp = field.getAnnotation(BqFieldPartition.class);
        BqFieldComputed bqc = field.getAnnotation(BqFieldComputed.class);
        int nonNulls = (bqf != null ? 1 : 0) + (bqp != null ? 1 : 0) + (bqc != null ? 1 : 0);
        if (nonNulls > 1)
            throw new IllegalStateException(
                "BqField can only be marked with one of @BqField, @BqFieldPartition, or @BqFieldComputed.");
        BqOrderer bqo = field.getAnnotation(BqOrderer.class);
        BqDescription bqd = field.getAnnotation(BqDescription.class);
        if (bqf != null)
            return new FieldSpec(field, bqf, bqo, bqd);
        if (bqp != null)
            return new FieldSpec(field, bqp, bqo, bqd);
        if (bqc != null)
            return new FieldSpec(field, bqc, bqo, bqd);

        if (bqo != null)
            throw new IllegalStateException(
                "BqField marked as @BqOrdere must also be marked with @BqField, @BqFieldPartition, or @BqFieldComputed.");
        return null;
    }

    private FieldSpec(Field field, BqFieldPartition ann, BqOrderer ord, BqDescription bqd) {
        this(true, !ann.include(), ann.key(), null, field, ann.name(), ord, PrimitiveFieldType.TIMESTAMP, ann.autoTimestamp(),
            bqd);
    }

    private FieldSpec(Field field, BqField ann, BqOrderer ord, BqDescription bqd) {
        this(false, false, ann.key(), null, field, ann.name(), ord, ann.type(), ann.autoTimestamp(), bqd);
    }

    private FieldSpec(Field field, BqFieldComputed ann, BqOrderer ord, BqDescription bqd) {
        this(false, true, ann.key(), ann.expression(), field, ann.name(), ord, ann.type(), false, bqd);
    }

    private FieldSpec(boolean isPartition, boolean isExcluded, boolean isKey, String computedExpression, Field field,
                      String annName, BqOrderer ord, String annType, boolean isAutoTimestamp, BqDescription bqd) {
        this(isPartition, isExcluded, isKey, computedExpression, field, annName, ord,
            annType.isEmpty() ? getColumnType(field.getType()) : PrimitiveFieldType.valueOf(annType), isAutoTimestamp, bqd);
    }

    private FieldSpec(boolean isPartition, boolean isExcluded, boolean isKey, String computedExpression, Field field,
                      String annName, BqOrderer ord, FieldType type, boolean isAutoTimestamp, BqDescription bqd) {

        this.isPartition = isPartition;
        this.isExcluded = isExcluded;
        this.isKey = isKey;
        this.computedExpression = computedExpression;
        this.isAutoTimestamp = isAutoTimestamp;

        this.orderNumber = ord == null ? null : ord.importance();
        this.orderAsc = ord != null && ord.asc();

        this.field = field;
        this.columnName = annName.isEmpty() ? field.getName() : annName;
        this.type = type;

        this.description = bqd == null ? null : bqd.value();
        this.descriptionTriggerUpdate = bqd != null && bqd.triggerUpdate();

        if (isAutoTimestamp && type != PrimitiveFieldType.TIMESTAMP)
            throw new IllegalStateException("AutoTimestamp field must be of type TIMESTAMP.");
    }

    void parse(TableRow row, Object object) throws IllegalAccessException {
        field.set(object, type.parse(row.get(columnName))); // set object.[fieldName]
    }

    void serialize(Object object, TableRow row) throws IllegalAccessException {
        if (isComputed())
            return;
        if (isAutoTimestamp()) // type is PrimitiveFieldType.TIMESTAMP.
            row.set(columnName, type.serialize(System.currentTimeMillis()));
        else {
            Object value = type.serialize(field.get(object));
            if (value != null)
                row.set(columnName, value); // write to row
        }
    }

    String getExpression() {
        if (isPartition())
            return "_PARTITIONTIME";
        if (isComputed())
            return computedExpression;
        return columnName;
    }

    String getAsExpression() {
        if (isPartition() && isExcluded())
            return "_PARTITIONTIME AS " + columnName;
        if (isComputed())
            return computedExpression + " AS " + columnName;
        return columnName;
    }

    boolean isPartition() {
        return isPartition;
    }
    boolean isExcluded() {
        return isExcluded;
    }
    boolean isKey() {
        return isKey;
    }
    boolean isComputed() {
        return null != computedExpression;
    }
    boolean isAutoTimestamp() {
        return isAutoTimestamp;
    }

    boolean isOrderer() {
        return null != orderNumber;
    }
    Integer getOrderNumber() {
        return orderNumber;
    }
    boolean isOrderAsc() {
        return orderAsc;
    }

    Field getField() {
        return field;
    }
    String getColumnName() {
        return columnName;
    }
    FieldType getType() {
        return type;
    }

    void outputBytes(DataOutputStream output, Object value) throws IllegalAccessException, IOException {
        getType().outputBytes(output, getField().get(value));
    }

    private static FieldType getColumnType(Class type) {
        if (String.class.equals(type))
            return PrimitiveFieldType.STRING;
        if (long.class.equals(type) || Long.class.equals(type))
            return PrimitiveFieldType.INTEGER;
        if (double.class.equals(type) || Double.class.equals(type))
            return PrimitiveFieldType.FLOAT;
        if (boolean.class.equals(type) || Boolean.class.equals(type))
            return PrimitiveFieldType.BOOLEAN;
        
        throw new IllegalArgumentException("Unable to assume column type from class: " + type.getCanonicalName());
    }

    public String getDescription() {
        return description;
    }

    public boolean getDescriptionTriggerUpdate() {
        return descriptionTriggerUpdate;
    }
}
