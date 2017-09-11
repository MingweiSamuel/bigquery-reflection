package com.mingweisamuel.bigqueryReflection;

//import org.junit.Test;
//import win.pickban.sona.bigquery.BqChampionBanStats;
//import win.pickban.sona.bigquery.BqChampionStats;
//import win.pickban.sona.bigquery.BqGeneralMatchStats;
//import win.pickban.sona.bigquery.BqMatch;

//import static org.junit.Assert.assertEquals;

import java.io.IOException;

/**
 * Testing {@link BqReflection}.
 */
public class BqReflectionTest {

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
