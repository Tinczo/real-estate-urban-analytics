package gtfs;

import gtfs.StopInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class GtfsAggregationReducer extends Reducer<Text, StopInfo, Text, StopInfo> {

    private final StopInfo finalInfo = new StopInfo();

    @Override
    protected void reduce(Text key, Iterable<StopInfo> values, Context context) throws IOException, InterruptedException {
        double totalScore = 0;
        boolean isFirst = true;

        for (StopInfo info : values) {
            totalScore += info.score;

            if (isFirst) {
                finalInfo.lat = info.lat;
                finalInfo.lon = info.lon;
                finalInfo.geoId = info.geoId;
                finalInfo.stopId = info.stopId;
                isFirst = false;
            }
        }

        finalInfo.score = totalScore;
        key.set(finalInfo.geoId);
        context.write(key, finalInfo);
    }
}