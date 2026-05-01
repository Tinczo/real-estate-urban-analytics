package gtfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mapper E1.3: czyta CSV z geocode_real_estate.py
 * (header: Nazwa dewelopera,Miejscowość,Ulica,Nr,Kod pocztowy,
 *           Cena m2,Cena całkowita,Powierzchnia,lat,lon)
 * Emituje (geoId, RealEstateRecord).
 */
public class RealEstateMapper extends Mapper<LongWritable, Text, Text, RealEstateRecord> {

    private final Text outKey = new Text();
    private final RealEstateRecord outVal = new RealEstateRecord();

    private boolean firstLine = true;

    // Indeksy kolumn (wykrywane z nagłówka)
    private int idxCity = -1, idxStreet = -1, idxNum = -1, idxZip = -1;
    private int idxPriceM2 = -1, idxPriceTot = -1, idxArea = -1;
    private int idxLat = -1, idxLon = -1;

    private static long offerCounter = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split(",", -1);

        // Parsuj nagłówek przy pierwszym wierszu
        if (firstLine) {
            firstLine = false;
            for (int i = 0; i < parts.length; i++) {
                String col = parts[i].trim().toLowerCase()
                        .replace("﻿", "")  // BOM
                        .replace("\"", "");
                switch (col) {
                    case "miejscowość": case "miejscowosc": idxCity = i; break;
                    case "ulica":       idxStreet = i; break;
                    case "nr":          idxNum = i; break;
                    case "kod pocztowy": idxZip = i; break;
                    case "cena m2":     idxPriceM2 = i; break;
                    case "cena całkowita": case "cena calkowita": idxPriceTot = i; break;
                    case "powierzchnia": idxArea = i; break;
                    case "lat":         idxLat = i; break;
                    case "lon":         idxLon = i; break;
                }
            }
            if (idxLat < 0 || idxLon < 0) {
                throw new IOException("Brak kolumn lat/lon w nagłówku CSV. Dostępne: " + line);
            }
            return;
        }

        if (idxLat < 0 || idxLon < 0 || parts.length <= Math.max(idxLat, idxLon)) {
            context.getCounter("RE_E13", "RECORDS_SKIPPED_BAD_FORMAT").increment(1);
            return;
        }

        try {
            double lat = Double.parseDouble(parts[idxLat].trim());
            double lon = Double.parseDouble(parts[idxLon].trim());

            if (lat == 0.0 && lon == 0.0) {
                context.getCounter("RE_E13", "RECORDS_SKIPPED_ZERO_COORDS").increment(1);
                return;
            }

            double cenaM2  = parseDouble(parts, idxPriceM2);
            double cenaTot = parseDouble(parts, idxPriceTot);
            double area    = parseDouble(parts, idxArea);
            String miasto  = idxCity >= 0 && idxCity < parts.length
                    ? parts[idxCity].trim() : "";

            // ID oferty: numer wiersza (brak oryginalnego ID w danych)
            String idOferty = String.valueOf(++offerCounter);

            outVal.set(idOferty, lat, lon, cenaM2, cenaTot, area, miasto);
            outKey.set(outVal.geoId);
            context.write(outKey, outVal);
            context.getCounter("RE_E13", "RECORDS_PROCESSED").increment(1);

        } catch (NumberFormatException e) {
            context.getCounter("RE_E13", "RECORDS_SKIPPED_PARSE_ERROR").increment(1);
        }
    }

    private double parseDouble(String[] parts, int idx) {
        if (idx < 0 || idx >= parts.length) return 0.0;
        String s = parts[idx].trim().replace(" ", "").replace(",", ".");
        if (s.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
