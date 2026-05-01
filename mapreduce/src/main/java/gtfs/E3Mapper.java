package gtfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper E3: czyta CSV z T2 (output E2Reducer).
 * Format wiersza: id_oferty,geoId,cena_m2,cena_cal,powierzchnia,miasto,
 *                 wsk_przep,ind_handlu,ind_gastro,ind_rekr,ind_eduk,wsp_halas
 * Emituje (miasto, IndicatorValues).
 */
public class E3Mapper extends Mapper<LongWritable, Text, Text, IndicatorValues> {

    private final Text outKey = new Text();
    private final IndicatorValues outVal = new IndicatorValues();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Format wyjścia E2Reducer: "miasto|geoId \t CSV"
        // TextOutputFormat zapisuje: key \t value
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Rozbij na klucz i wartość (separator \t z TextOutputFormat)
        String csvPart;
        if (line.contains("\t")) {
            csvPart = line.substring(line.indexOf('\t') + 1);
        } else {
            csvPart = line;
        }

        String[] parts = csvPart.split(",", -1);
        // Oczekiwane kolumny: id,geoId,cena_m2,cena_cal,pow,miasto,wsk,handlu,gastro,rekr,eduk,halas
        if (parts.length < 12) {
            context.getCounter("E3", "RECORDS_SKIPPED_BAD_FORMAT").increment(1);
            return;
        }

        try {
            String miasto = parts[5].trim();
            if (miasto.isEmpty()) {
                context.getCounter("E3", "RECORDS_SKIPPED_NO_CITY").increment(1);
                return;
            }

            outVal.cenaZaM2      = parseD(parts[2]);
            outVal.cenaCalkowita = parseD(parts[3]);
            outVal.powierzchnia  = parseD(parts[4]);
            outVal.wskPrzep      = parseD(parts[6]);
            outVal.indHandlu     = parseD(parts[7]);
            outVal.indGastro     = parseD(parts[8]);
            outVal.indRekr       = parseD(parts[9]);
            outVal.indEduk       = parseD(parts[10]);
            outVal.wspHalas      = parseD(parts[11]);

            outKey.set(miasto);
            context.write(outKey, outVal);
            context.getCounter("E3", "RECORDS_PROCESSED").increment(1);

        } catch (Exception e) {
            context.getCounter("E3", "RECORDS_SKIPPED_PARSE_ERROR").increment(1);
        }
    }

    private double parseD(String s) {
        try { return Double.parseDouble(s.trim()); }
        catch (NumberFormatException e) { return 0.0; }
    }
}
