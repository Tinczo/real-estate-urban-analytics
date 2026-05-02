package gtfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reducer E2: Reduce-side Join po geoId.
 * Dla każdej komórki geoId agreguje przystanki (T1.5), POI OSM (T1.2)
 * i oferty nieruchomości (T1.3), po czym oblicza wskaźniki dostępności
 * dla każdej oferty w tej komórce.
 *
 * UWAGA: Hadoop reużywa obiektu Writable w Iterable — każdy JoinRecord
 * musi być SKOPIOWANY (copyFrom) przed zapisem do listy.
 */
public class E2Reducer extends Reducer<Text, JoinRecord, Text, Text> {

    private static final double EARTH_RADIUS_M = 6_371_000.0;
    private static final double ALPHA_EDUKACJA = 0.002;

    private double walkingRadius;

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void setup(Context context) {
        walkingRadius = context.getConfiguration()
                .getDouble("pipeline.walking.radius.m", 800.0);
    }

    @Override
    protected void reduce(Text geoId, Iterable<JoinRecord> values, Context context)
            throws IOException, InterruptedException {

        List<JoinRecord> stops  = new ArrayList<>();
        List<JoinRecord> pois   = new ArrayList<>();
        List<JoinRecord> offers = new ArrayList<>();

        // KRYTYCZNE: Hadoop reużywa tego samego obiektu JoinRecord na każdej iteracji.
        // Musimy wykonać głęboką kopię przed zapisem do listy.
        for (JoinRecord rec : values) {
            JoinRecord copy = new JoinRecord();
            copy.copyFrom(rec);
            switch (rec.source) {
                case JoinRecord.SOURCE_T15: stops.add(copy);  break;
                case JoinRecord.SOURCE_T12: pois.add(copy);   break;
                case JoinRecord.SOURCE_T13: offers.add(copy); break;
            }
        }

        if (offers.isEmpty()) return;

        context.getCounter("E2", "CELLS_WITH_OFFERS").increment(1);
        context.getCounter("E2", "OFFERS_PROCESSED").increment(offers.size());
        context.getCounter("E2", "STOPS_IN_CELL").increment(stops.size());
        context.getCounter("E2", "POIS_IN_CELL").increment(pois.size());

        for (JoinRecord offer : offers) {
            double wskPrzep  = 0.0;
            double indHandlu = 0.0;
            double indGastro = 0.0;
            double indRekr   = 0.0;
            double indEduk   = 0.0;

            // Dla hałasu — szukamy najbliższej drogi
            double minDrogaDist = Double.MAX_VALUE;
            double wagaNajblizszejDrogi = 0.0;

            // wsk_przepustowosci = Σ(K_k / d_k) dla przystanków w promieniu
            for (JoinRecord stop : stops) {
                double d = haversine(offer.offerLat, offer.offerLon,
                        stop.stopLat, stop.stopLon);
                if (d > 0 && d <= walkingRadius) {
                    wskPrzep += stop.kK / d;
                }
            }

            // Wskaźniki POI
            for (JoinRecord poi : pois) {
                double d = haversine(offer.offerLat, offer.offerLon,
                        poi.poiLat, poi.poiLon);
                if (d <= 0 || d > walkingRadius) continue;

                double d2 = d * d;
                switch (poi.category) {
                    case "HANDEL":
                        indHandlu += poi.wagaObiektu / d2;
                        break;
                    case "GASTRO":
                        indGastro += poi.wagaObiektu / d2;
                        break;
                    case "REKREACJA":
                        indRekr += poi.wagaObiektu / d2;
                        break;
                    case "EDUKACJA":
                        // Model grawitacyjny z wykładniczym tłumieniem
                        indEduk += Math.exp(-ALPHA_EDUKACJA * d);
                        break;
                    case "DROGA":
                        // wsp_halas — tylko najbliższa droga
                        if (d < minDrogaDist) {
                            minDrogaDist = d;
                            wagaNajblizszejDrogi = poi.wagaObiektu;
                        }
                        break;
                }
            }

            // wsp_halas = W_road * L_lanes / D_min
            // wagaObiektu dla DROGA = W_road * lanes (zakodowane w OsmMapper)
            double wspHalas = (minDrogaDist < Double.MAX_VALUE && minDrogaDist > 0)
                    ? wagaNajblizszejDrogi / minDrogaDist
                    : 0.0;

            // Klucz: miasto|geoId dla ułatwienia E3 (partycja per miasto)
            outKey.set(offer.miasto + "|" + geoId.toString());

            // Wartość: CSV z wszystkimi wskaźnikami
            outVal.set(String.join(",",
                    offer.idOferty,
                    geoId.toString(),
                    fmt(offer.cenaZaM2),
                    fmt(offer.cenaCalkowita),
                    fmt(offer.powierzchnia),
                    offer.miasto,
                    fmt(wskPrzep),
                    fmt(indHandlu),
                    fmt(indGastro),
                    fmt(indRekr),
                    fmt(indEduk),
                    fmt(wspHalas)
            ));

            context.write(outKey, outVal);
        }
    }

    /**
     * Odległość Haversine w metrach między dwoma punktami (lat/lon w stopniach).
     */
    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double rLat1 = Math.toRadians(lat1);
        double rLat2 = Math.toRadians(lat2);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(rLat1) * Math.cos(rLat2)
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);

        return EARTH_RADIUS_M * 2 * Math.asin(Math.sqrt(a));
    }

    private static String fmt(double v) {
        return String.format(java.util.Locale.US, "%.6f", v);
    }
}
