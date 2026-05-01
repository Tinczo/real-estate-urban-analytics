package gtfs;

import com.github.davidmoten.geo.GeoHash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Mapper E2 — źródło T1.5 (przystanki GTFS, K_k).
 * Każdy przystanek emitowany pod własnym geoId ORAZ 8 sąsiednich komórek
 * (spatial buffering 1 poziom = ~1.2 km), żeby obsłużyć graniczne przypadki.
 */
public class JoinMapperT15 extends Mapper<Text, StopInfo, Text, JoinRecord> {

    private final Text outKey = new Text();
    private final JoinRecord outVal = new JoinRecord();

    @Override
    protected void map(Text key, StopInfo stop, Context context)
            throws IOException, InterruptedException {

        outVal.source  = JoinRecord.SOURCE_T15;
        outVal.stopLat = stop.lat;
        outVal.stopLon = stop.lon;
        outVal.kK      = stop.score;

        String ownGeoId = stop.geoId;

        // Wyemituj pod własnym geoId
        outKey.set(ownGeoId);
        context.write(outKey, outVal);
        context.getCounter("E2_T15", "RECORDS_EMITTED").increment(1);

        // Wyemituj pod 8 sąsiednich komórek
        List<String> neighbors = GeoHash.neighbours(ownGeoId);
        for (String neighbor : neighbors) {
            outKey.set(neighbor);
            context.write(outKey, outVal);
        }
        context.getCounter("E2_T15", "NEIGHBOR_RECORDS_EMITTED").increment(neighbors.size());
    }
}
