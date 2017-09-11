package win.pickban.maokai;

import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Converts timestamps to and from millis.
 */
public class TimestampUtils {

    private static final int MILLIS_PER_SECOND = 1000;
    private static final int NANOS_PER_MILLI = 1000_000;

    private TimestampUtils() {}

    public static Timestamp fromMillis(long millis) {
        return Timestamp.newBuilder()
            .setSeconds(millis / MILLIS_PER_SECOND)
            .setNanos((int) ((millis % MILLIS_PER_SECOND) * NANOS_PER_MILLI)).build();
    }

    public static long toMillis(Timestamp timestamp) {
        return timestamp.getSeconds() * MILLIS_PER_SECOND + timestamp.getNanos() / NANOS_PER_MILLI;
    }

    private static final DateTimeFormatter timestampParser = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 3, true)
            .appendLiteral(' ').appendZoneId().toFormatter();
    public static long parseBqTimestamp(String value) {
        try {
            return (long) (Double.parseDouble(value) * 1000);
        }
        catch (NumberFormatException e) {
            return Instant.from(timestampParser.parse(value)).toEpochMilli();
        }
    }
}
