package com.mingweisamuel.bigqueryReflection;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
//import win.pickban.sona.bigquery.BqChampionBanStats;
//import win.pickban.sona.bigquery.BqChampionStats;
//import win.pickban.sona.bigquery.BqGeneralMatchStats;
//import win.pickban.sona.bigquery.BqMatch;

//import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Testing {@link BqReflection}.
 */
public class BqReflectionTest {

    @Test
    public void testBasic() throws IOException {
        BqSummoner s = new BqSummoner();
        s.id = 987;
        s.accountId = 123;
        s.rank = 111;
        s.timestamp = System.currentTimeMillis();

        TableRow row = BqReflection.of(BqSummoner.class).serialize(s);
        System.out.println(row);
        System.out.println(s.timestamp);
        System.out.println(TimestampUtils.parseBqTimestamp(row.get("ts").toString()));
    }

    @Test
    public void testArray() {
        Map<String, Object> obj = ImmutableMap.<String, Object>builder()
            .put("id", "143")
            .put("_pt", System.currentTimeMillis())
            .put("queue", "420")
            .put("ts", System.currentTimeMillis())
            .put("data", Arrays.asList(
                ImmutableMap.builder()
                    .put("id", 498)
                    .put("queue", 420)
                    .put("rank", 2500)
                    .put("remakes", 0)
                    .put("wins", 1)
                    .put("losses", 1)
                    .build(),
                ImmutableMap.builder()
                    .put("id", 498)
                    .put("queue", 420)
                    .put("rank", 3000)
                    .put("remakes", 0)
                    .put("wins", 0)
                    .put("losses", 1)
                    .build(),
                ImmutableMap.builder()
                    .put("id", 498)
                    .put("queue", 420)
                    .put("rank", 2000)
                    .put("remakes", 0)
                    .put("wins", 2)
                    .put("losses", 2)
                    .build(),
                ImmutableMap.builder()
                    .put("id", 498)
                    .put("queue", 420)
                    .put("rank", 1000)
                    .put("remakes", 0)
                    .put("wins", 1)
                    .put("losses", 4)
                    .build(),
                ImmutableMap.builder()
                    .put("id", 516)
                    .put("queue", 420)
                    .put("rank", 2500)
                    .put("remakes", 0)
                    .put("wins", 0)
                    .put("losses", 2)
                    .build()

            ))
            .build();

        BqReflection<AggregatedStats> r = BqReflection.of(AggregatedStats.class);
        AggregatedStats as = r.parse(obj);

        System.out.println(as.data);
    }

//    @Test
//    public void test() throws IOException {
//        BqReflection<BqMatch> r = BqReflection.of(BqMatch.class);
//        BqMatch m1 = new BqMatch();
//        m1.timestamp = 123;
//        m1.id = 456;
//        m1.dataRead = 789;
//
//        BqMatch m2 = r.parse(r.serialize(m1));
//        assertEquals(m1.timestamp, m2.timestamp);
//        assertEquals(m1.id, m2.id);
//        assertEquals(m1.dataRead, m2.dataRead);
//    }
//
//    @Test
//    public void testSql() {
//        String datasetName = "league_na";
//        String lookbehindString = "2017-08-16";
//        long earliestMatchTime = 1502845462238L;
//
//        String oldStr = String.format(
//            "SELECT dataRead, id, ts\n" +
//                "FROM (\n" +
//                "  SELECT dataRead, id, ts,\n" +
//                "  ROW_NUMBER() OVER(PARTITION BY id ORDER BY %s DESC) AS _rank\n" +
//                "  FROM %s.%s\n" +
//                "  WHERE _PARTITIONTIME >= TIMESTAMP('%s')\n" +
//                ")\n" +
//                "WHERE _rank=1\n",
//            BqMatch.K_DATA_READ, datasetName, BqMatch.TABLE, lookbehindString);
//        String newStr = BqReflection.getSql(BqMatch.class, datasetName, earliestMatchTime);
//        assertEquals(oldStr, newStr);
//
//        oldStr = String.format(
//            "SELECT _pt, id, losses, queue, rank, remakes, role, surrenderLosses, surrenderWins, ts, wins\n" +
//            "FROM (\n" +
//            "  SELECT _PARTITIONTIME AS _pt, id, losses, queue, rank, remakes, role, surrenderLosses, surrenderWins, ts, wins,\n" +
//            "  ROW_NUMBER() OVER(PARTITION BY _PARTITIONTIME, id, queue, rank, role ORDER BY ts DESC) AS _rank\n" +
//            "  FROM %s.%s\n" +
//            "  WHERE _PARTITIONTIME >= TIMESTAMP('%s')\n" +
//            ")\n" +
//            "WHERE _rank=1\n",
//            datasetName, BqChampionStats.TABLE, lookbehindString);
//        newStr = BqReflection.getSql(BqChampionStats.class, datasetName, earliestMatchTime);
//        assertEquals(oldStr, newStr);
//
//        oldStr = String.format(
//            "SELECT _pt, blueWins, gamesCompleted, queue, rank, remakes, ts\n" +
//            "FROM (\n" +
//            "  SELECT _PARTITIONTIME AS _pt, blueWins, gamesCompleted, queue, rank, remakes, ts,\n" +
//            "  ROW_NUMBER() OVER(PARTITION BY _PARTITIONTIME, queue, rank ORDER BY ts DESC) AS _rank\n" +
//            "  FROM %s.%s\n" +
//            "  WHERE _PARTITIONTIME >= TIMESTAMP('%s')\n" +
//            ")\n" +
//            "WHERE _rank=1\n",
//        datasetName, BqGeneralMatchStats.TABLE, lookbehindString);
//        newStr = BqReflection.getSql(BqGeneralMatchStats.class, datasetName, earliestMatchTime);
//        assertEquals(oldStr, newStr);
//
//        oldStr = String.format(
//            "SELECT _pt, bans, gameBans, id, queue, rank, ts\n" +
//            "FROM (\n" +
//            "  SELECT _PARTITIONTIME AS _pt, bans, gameBans, id, queue, rank, ts,\n" +
//            "  ROW_NUMBER() OVER(PARTITION BY _PARTITIONTIME, id, queue, rank ORDER BY ts DESC) AS _rank\n" +
//            "  FROM %s.%s\n" +
//            "  WHERE _PARTITIONTIME >= TIMESTAMP('%s')\n" +
//            ")\n" +
//            "WHERE _rank=1\n",
//            datasetName, BqChampionBanStats.TABLE, lookbehindString);
//        newStr = BqReflection.getSql(BqChampionBanStats.class, datasetName, earliestMatchTime);
//        assertEquals(oldStr, newStr);
//    }
}
