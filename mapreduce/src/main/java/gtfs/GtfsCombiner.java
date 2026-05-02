package gtfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner dla E1.5: sumuje score per stopId BEZ zmiany klucza.
 *
 * GtfsAggregationReducer nie może być użyty jako Combiner, bo robi
 * key.set(finalInfo.geoId) przed context.write — zmienia klucz z stopId
 * na geoId, co psuje shuffle. Ten Combiner zachowuje oryginalny klucz (stopId).
 */
public class GtfsCombiner extends Reducer<Text, StopInfo, Text, StopInfo> {

    private final StopInfo combined = new StopInfo();

    @Override
    protected void reduce(Text key, Iterable<StopInfo> values, Context context)
            throws IOException, InterruptedException {

        double totalScore = 0;
        boolean isFirst = true;

        for (StopInfo info : values) {
            totalScore += info.score;
            if (isFirst) {
                combined.lat    = info.lat;
                combined.lon    = info.lon;
                combined.geoId  = info.geoId;
                combined.stopId = info.stopId;
                isFirst = false;
            }
        }

        combined.score = totalScore;
        // klucz NIE jest modyfikowany — zostaje stopId (oryginalne key)
        context.write(key, combined);
    }
}
