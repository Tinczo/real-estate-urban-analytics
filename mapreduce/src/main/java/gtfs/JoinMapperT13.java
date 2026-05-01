package gtfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper E2 — źródło T1.3 (oferty nieruchomości).
 * Emituje pod własnym geoId.
 */
public class JoinMapperT13 extends Mapper<Text, RealEstateRecord, Text, JoinRecord> {

    private final Text outKey = new Text();
    private final JoinRecord outVal = new JoinRecord();

    @Override
    protected void map(Text key, RealEstateRecord offer, Context context)
            throws IOException, InterruptedException {

        outVal.source        = JoinRecord.SOURCE_T13;
        outVal.idOferty      = offer.idOferty;
        outVal.miasto        = offer.miasto;
        outVal.offerLat      = offer.lat;
        outVal.offerLon      = offer.lon;
        outVal.cenaZaM2      = offer.cenaZaM2;
        outVal.cenaCalkowita = offer.cenaCalkowita;
        outVal.powierzchnia  = offer.powierzchnia;

        outKey.set(offer.geoId);
        context.write(outKey, outVal);
        context.getCounter("E2_T13", "RECORDS_EMITTED").increment(1);
    }
}
