package com.mingweisamuel.bigqueryReflection;

import com.google.api.gax.paging.Page;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BqReflection<T extends Serializable> {

    private static final Map<String, BqReflection> cache = new HashMap<>();
    private static final Map<String, String> tableNames = new HashMap<>();
    @SuppressWarnings("unchecked")
    public synchronized static <T extends Serializable> BqReflection<T> of(Class<T> clazz) {
        if (cache.containsKey(clazz.getCanonicalName()))
            return cache.get(clazz.getCanonicalName());
        BqReflection<T> result = new BqReflection<>(clazz);
        if (tableNames.containsKey(result.tableName))
            throw new IllegalStateException(String.format("Multiple tables with name \"%s\", existing from class %s, " +
                "new from class %s.", result.tableName, tableNames.get(result.tableName), clazz.getCanonicalName()));
        cache.put(clazz.getCanonicalName(), result);
        tableNames.put(result.tableName, clazz.getCanonicalName());
        return result;
    }

    private final Class<T> clazz;
    private final String tableName;
    private final ImmutableList<FieldSpec> fields;
    private final ImmutableList<FieldSpec> ordererFields;
    private final FieldSpec partitionField;

    private BqReflection(Class<T> clazz) {
        this.clazz = clazz;

        BqTable bqTable = clazz.getDeclaredAnnotation(BqTable.class);
        if (bqTable == null)
            throw new IllegalArgumentException("Class does not have @BqTable annotation.");
        if (!bqTable.value().isEmpty())
            this.tableName = bqTable.value();
        else
            this.tableName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, clazz.getSimpleName());
        //System.out.println(tableName);

        FieldSpec partition = null;
        Set<String> columnNames = new HashSet<>();
        List<FieldSpec> fields = new ArrayList<>();
        List<FieldSpec> ordererFields = new ArrayList<>();
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                FieldSpec spec = FieldSpec.get(field);
                if (spec == null)
                    continue;

                if (spec.isOrderer())
                    ordererFields.add(spec);

                if (spec.isPartition()) {
                    if (partition != null)
                        throw new IllegalStateException(String.format("Multiple partitions: \"%s\" and \"%s\".",
                            partition.getField().getName(), field.getName()));
                    partition = spec;
                }

                if (columnNames.contains(spec.getColumnName()))
                    throw new IllegalStateException(
                        "Multiple fields with column name \"" + spec.getColumnName() + "\".");
                columnNames.add(spec.getColumnName());

                fields.add(spec);
            }
        }
        this.partitionField = partition;

        fields.sort(Comparator.comparing(FieldSpec::getColumnName));
        this.fields = ImmutableList.copyOf(fields);

//        if (ordererFields.isEmpty())
//            throw new IllegalStateException(clazz.getName() + ": At least one field must be specified as @BqOrderer.");
        ordererFields.sort(Comparator.comparingInt(FieldSpec::getOrderNumber));
        this.ordererFields = ImmutableList.copyOf(ordererFields);
    }

    public byte[] getKey(T value) {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(bytesOut);
        try {
            for (FieldSpec field : fields) {
                if (field.isKey())
                    field.outputBytes(dataOut, value);
            }
            dataOut.flush();
            return bytesOut.toByteArray();
        } catch (IllegalAccessException | IOException e) {
            throw new IllegalStateException(clazz.getName() + ": Unable to serialize instance.", e);
        }
    }

    public T parse(TableRow row) {
        try {
            Constructor<T> constructor = clazz.getConstructor();
            T object = constructor.newInstance();
            for (FieldSpec field : fields)
                field.parse(row, object);
            return object;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Cannot parse class without no-arg constructor: " + clazz.getCanonicalName(), e);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException("Unable to parse class " + clazz.getCanonicalName() + " from row " + row, e);
        }
    }

    public T parse(Schema schema, List<FieldValue> row) {
        Map<String, FieldValue> columns = new HashMap<>();
        for (int i = 0; i < row.size(); i++) {
            String name = schema.getFields().get(i).getName();
            FieldValue fieldValue = row.get(i);
            columns.put(name, fieldValue);
        }
        try {
            Constructor<T> constructor = clazz.getConstructor();
            T object = constructor.newInstance();
            for (FieldSpec field : fields)
                field.parse(columns, object);
            return object;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Cannot parse class without no-arg constructor: " + clazz.getCanonicalName(), e);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException("Unable to parse class " + clazz.getCanonicalName() + " from row " + row, e);
        }
    }

    public TableRow serialize(T value) {
        try {
            TableRow row = new TableRow();
            for (FieldSpec field : fields)
                field.serialize(value, row);
            return row;
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Unable to serialize class " + clazz.getCanonicalName(), e);
        }
    }

    public static <T extends Serializable> SimpleFunction<TableRow, T> parser(Class<T> clazz) {
        return new SimpleFunction<TableRow, T>() {
            @Override
            public T apply(TableRow input) {
                return of(clazz).parse(input);
            }
        };
    }
    public static <T extends Serializable> SimpleFunction<T, TableRow> serializer(Class<T> clazz) {
        return new SimpleFunction<T, TableRow>() {
            @Override
            public TableRow apply(T input) {
                return of(clazz).serialize(input);
            }
        };
    }

    public static <T extends Serializable> PTransform<PCollection<T>, PCollection<KV<byte[], T>>> key(
            Class<T> clazz) {

        return new PTransform<PCollection<T>, PCollection<KV<byte[], T>>>() {
            @Override
            public PCollection<KV<byte[], T>> expand(PCollection<T> input) {
                return input.apply(MapElements.via(new SimpleFunction<T, KV<byte[], T>>() {
                    @Override
                    public KV<byte[], T> apply(T input) {
                        return KV.of(of(clazz).getKey(input), input);
                    }
                }));
            }
        };
    }

    public static <T extends Serializable> String getSql(Class<T> clazz, String datasetName, long time,
        SqlBiPredicate timePredicate) {

        return of(clazz).getSql(datasetName, time, timePredicate);
    }

    public String getSql(String datasetName, long time, SqlBiPredicate timePredicate) {
        StringBuilder outerBuilder = new StringBuilder();
        StringBuilder innerBuilder = new StringBuilder();
        StringBuilder keyBuilder = new StringBuilder();
        for (FieldSpec field : fields) {
            outerBuilder.append(", ").append(field.getColumnName());
            innerBuilder.append(", ").append(field.getAsExpression());
            if (field.isKey())
                keyBuilder.append(", ").append(field.getExpression());
        }
        String outer = outerBuilder.length() < 2 ? "" : outerBuilder.substring(2);
        String inner = innerBuilder.length() < 2 ? "" : innerBuilder.substring(2);
        String key = keyBuilder.length() < 2 ? "" : keyBuilder.substring(2);

        String order = "";
        if (!ordererFields.isEmpty()) {
            StringBuilder orderBuilder = new StringBuilder(" ORDER BY ");
            for (FieldSpec field : ordererFields) {
                orderBuilder.append(field.getColumnName())
                    .append(field.isOrderAsc() ? " ASC" : " DESC").append(", ");
            }
            order = orderBuilder.substring(0, orderBuilder.length() - 2);
        }

        String dateString = DateTimeFormatter.ISO_LOCAL_DATE.format(
            Instant.ofEpochMilli(time).atOffset(ZoneOffset.UTC));

        return
            "SELECT " + outer + "\n" +
            "FROM (\n" +
            "  SELECT " + inner + ",\n" +
            "  ROW_NUMBER() OVER(PARTITION BY " + key + order + ") AS _rank\n" +
            "  FROM " + datasetName + '.' + tableName + "\n" +
            "  WHERE _PARTITIONTIME " + timePredicate.op + " TIMESTAMP('" + dateString + "')\n" +
            ")\n" +
            "WHERE _rank=1\n";
    }

    public static <T extends Serializable> PTransform<PBegin, PCollection<T>> read(
            Class<T> clazz, String datasetName, long minTime) {

        System.out.println(
            '"' + BqReflection.getSql(clazz, datasetName, minTime, SqlBiPredicate.GREATER_THAN_OR_EQUAL_TO) + '"');

        return new PTransform<PBegin, PCollection<T>>() {
            @Override
            public PCollection<T> expand(PBegin input) {
                return input.getPipeline()
                    .apply("Read", BigQueryIO.read()
                        .fromQuery(BqReflection.getSql(clazz, datasetName, minTime, SqlBiPredicate.GREATER_THAN_OR_EQUAL_TO))
                        .usingStandardSql())
                    .apply("Deserialize", MapElements.via(parser(clazz)));
            }
        };
    }

    public static <T extends Serializable> PTransform<PCollection<T>, WriteResult> write(
            Class<T> clazz, String datasetName) {

        return new PTransform<PCollection<T>, WriteResult>() {
            @Override
            public WriteResult expand(PCollection<T> input) {
                return input
                    .apply("Serialize", MapElements.via(serializer(clazz)))
                    .apply("Write", BigQueryIO.<T>writeTableRows()
                        .to(value -> {
                            BqReflection<T> reflection = of(clazz);
                            if (reflection.partitionField == null)
                                return new TableDestination(datasetName + '.' + reflection.tableName, null);
                            long partitionTime = TimestampUtils.parseBqTimestamp(value.getValue().get(
                                reflection.partitionField.getColumnName()).toString());
                            String dateString = DateTimeFormatter.ofPattern("yyyyMMdd").format(
                                Instant.ofEpochMilli(partitionTime).atOffset(ZoneOffset.UTC));
                            // Remove excluded columns from row if not included.
                            if (reflection.partitionField.isExcluded())
                                value.getValue().remove(reflection.partitionField.getColumnName());
                            return new TableDestination(
                                datasetName + '.' + reflection.tableName + '$' + dateString, null);
                        })
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
            }
        };
    }

    private Iterable<FieldSpec> includedFields() {
        //noinspection StaticPseudoFunctionalStyleMethod
        return Iterables.filter(fields, field -> !field.isExcluded());
    }

    private TableDefinition getTableDefinition(boolean partitioned, boolean required) {
        System.out.println(this.tableName);
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (FieldSpec field : includedFields()) {
            System.out.println("  " + field.getColumnName());
            com.google.cloud.bigquery.Field.Builder fieldBuilder = com.google.cloud.bigquery.Field
                .newBuilder(field.getColumnName(), field.getType().getBqType());
            if (field.getDescription() != null)
                fieldBuilder.setDescription(field.getDescription());
            if (required)
                fieldBuilder.setMode(com.google.cloud.bigquery.Field.Mode.REQUIRED);
            schemaBuilder.addField(fieldBuilder.build());
        }
        StandardTableDefinition.Builder tableDefBuilder = StandardTableDefinition.newBuilder();
        if (partitioned)
            tableDefBuilder.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));
        tableDefBuilder.setSchema(schemaBuilder.build());
        return tableDefBuilder.build();
    }

    private Table createTable(BigQuery bigQuery, String datasetId) {
        TableDefinition tableDef = getTableDefinition(partitionField != null, true);
        return bigQuery.create(TableInfo.of(TableId.of(datasetId, tableName), tableDef));
    }

    private Table updateTable(BigQuery bigQuery, String datasetId, Table existingTable) {
            existingTable = existingTable.reload();
        Schema existingSchema = existingTable.getDefinition().getSchema();
        Map<String, com.google.cloud.bigquery.Field> existingFields = existingSchema.getFields().stream()
            .collect(Collectors.toMap(com.google.cloud.bigquery.Field::getName, Function.identity()));
        boolean needsUpdate = false;
        for (FieldSpec field : includedFields()) {
            com.google.cloud.bigquery.Field existingField = existingFields.get(field.getColumnName());
            if (existingField == null)
                needsUpdate = true;
            else if (field.getDescriptionTriggerUpdate() && existingField.getDescription().equals(field.getDescription()))
                needsUpdate = true;
            else if (!existingField.getType().equals(field.getType().getBqType()))
                throw new IllegalStateException(String.format("Types do not match for field \"%s\", local: %s, remote: %s.",
                    field.getColumnName(), field.getType(), existingField.getType()));

        }
        TableDefinition tableDef = getTableDefinition(partitionField != null, false);
        if (needsUpdate)
            return bigQuery.update(TableInfo.of(TableId.of(datasetId, tableName), tableDef));
        return existingTable;
    }

    public static void createOrUpdateTables(BigQuery bigQuery, String datasetId, Class... classes) {

        HashMap<String, Table> existingTables = new HashMap<>();
        Page<Table> tablePage = bigQuery.listTables(datasetId);
        for (; tablePage != null; tablePage = tablePage.getNextPage()) {
            for (Table table : tablePage.iterateAll()) {
                existingTables.put(table.getTableId().getTable(), table);
            }
        }

        for (Class clazz : classes) {
            if (!Serializable.class.isAssignableFrom(clazz))
                throw new IllegalArgumentException(
                    "Class " + clazz.getCanonicalName() + " must implement Serializable.");
            @SuppressWarnings("unchecked")
            BqReflection<?> ref = of(clazz);
            Table existingTable = existingTables.get(ref.tableName);
            if (existingTable == null) {
                ref.createTable(bigQuery, datasetId);
            }
            else {
                ref.updateTable(bigQuery, datasetId, existingTable);
            }
        }
    }
}
