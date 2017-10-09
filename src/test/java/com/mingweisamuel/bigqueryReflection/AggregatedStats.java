package com.mingweisamuel.bigqueryReflection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@BqTable(AggregatedStats.TABLE)
public class AggregatedStats implements Serializable {

    public static final String TABLE = "aggStats";

    public static final String K_ID = "id";
    @BqDescription("Champion ID.")
    @BqField(name = K_ID, key = true)
    public long id;

    public static final String K_PT = "_pt";
    @BqFieldPartition(key = true, name = K_PT)
    public long partitionTime;

    public static final String K_QUEUE = "queue";
    @BqField(name = K_QUEUE, key = true)
    public long queue;

    public static final String K_TIMESTAMP = "ts";
    @BqOrderer
    @BqField(name = K_TIMESTAMP, type = "TIMESTAMP", autoTimestamp = true)
    public long timestamp; // not key

    @BqArray(type = DataElement.class)
    public List<DataElement> data = new LinkedList<>();

    public static class DataElement implements Serializable {

        public static final String K_ID = "id";
        @BqDescription("Champion ID.")
        @BqField(name = K_ID, key = true)
        public long id;

        //region WIN/LOSS
        public static final String K_WINS = "wins";
        @BqDescription("Wins by outer champ.")
        @BqField(name = K_WINS)
        public long wins;

        public static final String K_LOSSES = "losses";
        @BqDescription("Losses by outer champ.")
        @BqField(name = K_LOSSES)
        public long losses;

        public static final String K_REMAKES = "remakes";
        @BqDescription("Remakes, mutually exclusive with wins and losses")
        @BqField(name = K_REMAKES)
        public long remakes;
        //endregion

        @Override
        public String toString() {
            return "DataElement{" +
                "id=" + id +
                ", wins=" + wins +
                ", losses=" + losses +
                ", remakes=" + remakes +
                '}';
        }
    }

    @Override
    public String toString() {
        return "AggregatedStats{" +
            "id=" + id +
            ", partitionTime=" + partitionTime +
            ", queue=" + queue +
            ", timestamp=" + timestamp +
            ", data=" + data +
            '}';
    }

    //    //region SURRENDERS TODO
//    public static final String K_SURRENDER_WINS_0 = "surrenderWins0";
//    @BqField(name = K_SURRENDER_WINS_0)
//    public long surrenderWins0;
//
//    public static final String K_SURRENDER_WINS_1 = "surrenderWins1";
//    @BqField(name = K_SURRENDER_WINS_1)
//    public long surrenderWins1;
//    //endregion
//
//    public int newStats = 0;
}
