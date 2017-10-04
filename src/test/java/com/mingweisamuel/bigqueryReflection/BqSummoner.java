package com.mingweisamuel.bigqueryReflection;

import java.io.Serializable;

/**
 * One-to-one representation of a summoner in the Big Query store.
 */
@BqTable(BqSummoner.TABLE)
public class BqSummoner implements Serializable {

    public static final String TABLE = "summoner";

    public static final String K_ID = "id";
    public static final String K_ACCOUNT_ID = "accountId";
    public static final String K_TS = "ts";
    public static final String K_RANKS = "rank";
    public static final String K_LEAGUE_ID = "leagueId";

    // Property read from SELECT query.
    public static final String K_NEEDS_UPDATE = "_needsUpdate";

    @BqDescription("Time this row was updated, used to keep track of row updates.")
    @BqOrderer
    //@BqFieldPartition(name = K_TS, autoTimestamp = false, include = true)
    @BqField(name = K_TS, type = "TIMESTAMP", autoTimestamp = false)
    public long timestamp;

    @BqDescription("Summoner ID.")
    @BqField(name = K_ID, key = true)
    public long id;

    @BqDescription("Riot Games account ID.")
    @BqField(name = K_ACCOUNT_ID)
    public long accountId;

    @BqDescription("Rank encoded using EncodedRank.")
    @BqField(name = K_RANKS)
    public long rank;

    @BqDescription("Fuzzy league identifier for grouping leagues.")
    @BqField(name = K_LEAGUE_ID)
    public Long leagueId;

    @BqFieldComputed(name = K_NEEDS_UPDATE)
    public boolean needsUpdate;

    @Override
    public String toString() {
        return "BqSummoner{" +
            "id=" + id +
            ", accountId=" + accountId +
            '}';
    }
}
