package gtfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer E3: oblicza macierz korelacji Pearsona per miasto.
 * Dla każdego miasta akumuluje statystyki potrzebne do wzoru Pearsona
 * (n, ΣX, ΣY, ΣX², ΣY², ΣXY) dla każdej pary (wskaźnik × zmienna cenowa).
 *
 * Wyjście: miasto,wskaznik,korelacja_cena_m2,korelacja_cena,korelacja_powierzchnia
 */
public class E3Reducer extends Reducer<Text, IndicatorValues, Text, Text> {

    private static final String[] INDICATORS = {
            "wsk_przepustowosci", "ind_handlu", "ind_gastro",
            "ind_rekreacji", "ind_edukacji", "wsp_halas"
    };

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text miasto, Iterable<IndicatorValues> values, Context context)
            throws IOException, InterruptedException {

        long n = 0;

        // Sumy dla zmiennych cenowych (Y): cena_m2, cena_cal, powierzchnia
        double sumCenaM2 = 0, sumCenaCal = 0, sumPow = 0;
        double sumCenaM2_2 = 0, sumCenaCal_2 = 0, sumPow_2 = 0;

        // Sumy dla wskaźników (X): 6 wskaźników
        double[] sumX    = new double[6];
        double[] sumX2   = new double[6];
        double[] sumXY_m2  = new double[6];
        double[] sumXY_cal = new double[6];
        double[] sumXY_pow = new double[6];

        for (IndicatorValues v : values) {
            n++;
            double cM2  = v.cenaZaM2;
            double cCal = v.cenaCalkowita;
            double pow  = v.powierzchnia;

            sumCenaM2   += cM2;  sumCenaM2_2  += cM2 * cM2;
            sumCenaCal  += cCal; sumCenaCal_2 += cCal * cCal;
            sumPow      += pow;  sumPow_2     += pow * pow;

            double[] xs = { v.wskPrzep, v.indHandlu, v.indGastro,
                             v.indRekr,  v.indEduk,   v.wspHalas };

            for (int i = 0; i < 6; i++) {
                double x = xs[i];
                sumX[i]      += x;
                sumX2[i]     += x * x;
                sumXY_m2[i]  += x * cM2;
                sumXY_cal[i] += x * cCal;
                sumXY_pow[i] += x * pow;
            }
        }

        if (n < 2) return;

        context.getCounter("E3", "CITIES_PROCESSED").increment(1);
        context.getCounter("E3", "OFFERS_PER_CITY_TOTAL").increment(n);

        for (int i = 0; i < 6; i++) {
            double rM2  = pearson(n, sumX[i], sumCenaM2,  sumX2[i], sumCenaM2_2,  sumXY_m2[i]);
            double rCal = pearson(n, sumX[i], sumCenaCal, sumX2[i], sumCenaCal_2, sumXY_cal[i]);
            double rPow = pearson(n, sumX[i], sumPow,     sumX2[i], sumPow_2,     sumXY_pow[i]);

            outKey.set(miasto.toString());
            outVal.set(String.join(",",
                    INDICATORS[i],
                    fmt(rM2),
                    fmt(rCal),
                    fmt(rPow)
            ));
            context.write(outKey, outVal);
        }
    }

    /**
     * Korelacja Pearsona ze skumulowanych sum (numerycznie stabilna).
     * r = (n·ΣXY − ΣX·ΣY) / sqrt((n·ΣX² − (ΣX)²) · (n·ΣY² − (ΣY)²))
     */
    private static double pearson(long n, double sumX, double sumY,
                                   double sumX2, double sumY2, double sumXY) {
        double num = n * sumXY - sumX * sumY;
        double denX = n * sumX2 - sumX * sumX;
        double denY = n * sumY2 - sumY * sumY;
        if (denX <= 0 || denY <= 0) return 0.0;
        return num / Math.sqrt(denX * denY);
    }

    private static String fmt(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return "0.000000";
        return String.format(java.util.Locale.US, "%.6f", v);
    }
}
