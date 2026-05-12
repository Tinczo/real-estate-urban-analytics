package gtfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mapper E1.3: czyta CSV z geocode_real_estate.py
 * (header: Nazwa dewelopera,Miejscowość,Ulica,Nr,Kod pocztowy,
 *           Cena m2,Cena całkowita,Powierzchnia,lat,lon)
 * Emituje (geoId, RealEstateRecord).
 */
public class RealEstateMapper extends Mapper<LongWritable, Text, Text, RealEstateRecord> {

    private final Text outKey = new Text();
    private final RealEstateRecord outVal = new RealEstateRecord();

    // Indeksy kolumn (wykrywane z nagłówka)
    private int idxCity = -1, idxStreet = -1, idxNum = -1, idxZip = -1;
    private int idxPriceM2 = -1, idxPriceTot = -1, idxArea = -1;
    private int idxLat = -1, idxLon = -1;

    private long offerCounter = 0;
    private String taskPrefix = "0_";

    @Override
    protected void setup(Context context) {
        taskPrefix = context.getTaskAttemptID().getTaskID().getId() + "_";
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = splitCsv(line);

        // Nagłówek: wykryj po treści, nie po pozycji (działa też przy splitach)
        if (idxLat < 0) {
            if (line.toLowerCase().contains("lat") && line.toLowerCase().contains("lon")) {
                // To jest wiersz nagłówkowy — sparsuj indeksy
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
                return;
            } else {
                // Split dostał środek pliku bez nagłówka — użyj stałych pozycji
                // geocode_real_estate.py zawsze pisze: Nazwa,Miejscowość,Ulica,Nr,Kod,CenaM2,CenaCal,Pow,lat,lon
                idxCity = 1; idxStreet = 2; idxNum = 3; idxZip = 4;
                idxPriceM2 = 5; idxPriceTot = 6; idxArea = 7; idxLat = 8; idxLon = 9;
            }
        }

        if (parts.length <= Math.max(idxLat, idxLon)) {
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

            String idOferty = taskPrefix + (++offerCounter);

            outVal.set(idOferty, lat, lon, cenaM2, cenaTot, area, miasto);
            outKey.set(outVal.geoId);
            context.write(outKey, outVal);
            context.getCounter("RE_E13", "RECORDS_PROCESSED").increment(1);

        } catch (NumberFormatException e) {
            context.getCounter("RE_E13", "RECORDS_SKIPPED_PARSE_ERROR").increment(1);
        }
    }

    private static String[] splitCsv(String line) {
        List<String> result = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder current = new StringBuilder();
        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ';' && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());
        return result.toArray(new String[0]);
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
