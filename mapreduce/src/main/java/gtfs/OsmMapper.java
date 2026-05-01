package gtfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper E1.2: czyta CSV z osm_to_csv.py (header: lat,lon,category,waga_obiektu)
 * i emituje (geoId, OsmRecord).
 */
public class OsmMapper extends Mapper<LongWritable, Text, Text, OsmRecord> {

    private final Text outKey = new Text();
    private final OsmRecord outVal = new OsmRecord();

    private boolean firstLine = true;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Pomiń header (kolumny: lat,lon,category,waga_obiektu)
        if (firstLine && line.startsWith("lat")) {
            firstLine = false;
            return;
        }
        firstLine = false;

        String[] parts = line.split(",", -1);
        if (parts.length < 4) {
            context.getCounter("OSM_E12", "RECORDS_SKIPPED_BAD_FORMAT").increment(1);
            return;
        }

        try {
            double lat = Double.parseDouble(parts[0].trim());
            double lon = Double.parseDouble(parts[1].trim());
            String category = parts[2].trim();
            double waga = Double.parseDouble(parts[3].trim());

            if (lat == 0.0 && lon == 0.0) {
                context.getCounter("OSM_E12", "RECORDS_SKIPPED_ZERO_COORDS").increment(1);
                return;
            }

            outVal.set(lat, lon, category, waga);
            outKey.set(outVal.geoId);
            context.write(outKey, outVal);
            context.getCounter("OSM_E12", "RECORDS_PROCESSED").increment(1);

        } catch (NumberFormatException e) {
            context.getCounter("OSM_E12", "RECORDS_SKIPPED_PARSE_ERROR").increment(1);
        }
    }
}
