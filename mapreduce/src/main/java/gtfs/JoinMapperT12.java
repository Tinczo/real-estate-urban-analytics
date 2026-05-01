package gtfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper E2 — źródło T1.2 (obiekty OSM).
 * Emituje pod własnym geoId — ekspansja do sąsiadów jest po stronie T1.5.
 */
public class JoinMapperT12 extends Mapper<Text, OsmRecord, Text, JoinRecord> {

    private final Text outKey = new Text();
    private final JoinRecord outVal = new JoinRecord();

    @Override
    protected void map(Text key, OsmRecord poi, Context context)
            throws IOException, InterruptedException {

        outVal.source      = JoinRecord.SOURCE_T12;
        outVal.poiLat      = poi.lat;
        outVal.poiLon      = poi.lon;
        outVal.category    = poi.category;
        outVal.wagaObiektu = poi.wagaObiektu;

        outKey.set(poi.geoId);
        context.write(outKey, outVal);
        context.getCounter("E2_T12", "RECORDS_EMITTED").increment(1);
    }
}
