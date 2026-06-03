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

        double[] meanX = new double[6];
        double[] M2_X = new double[6];

        double[] meanY = new double[3];
        double[] M2_Y = new double[3];

        double[][] covXY = new double[6][3];

        for (IndicatorValues v : values) {
            n++;
            double[] y = { v.cenaZaM2, v.cenaCalkowita, v.powierzchnia };
            double[] x = { v.wskPrzep, v.indHandlu, v.indGastro,
                           v.indRekr,  v.indEduk,   v.wspHalas };

            double[] dx = new double[6];
            for (int i = 0; i < 6; i++) {
                dx[i] = x[i] - meanX[i];
                meanX[i] += dx[i] / n;
                M2_X[i] += dx[i] * (x[i] - meanX[i]);
            }

            double[] dy = new double[3];
            for (int j = 0; j < 3; j++) {
                dy[j] = y[j] - meanY[j];
                meanY[j] += dy[j] / n;
                M2_Y[j] += dy[j] * (y[j] - meanY[j]);
            }

            for (int i = 0; i < 6; i++) {
                for (int j = 0; j < 3; j++) {
                    covXY[i][j] += dx[i] * (y[j] - meanY[j]);
                }
            }
        }

        if (n < 2) return;

        context.getCounter("E3", "CITIES_PROCESSED").increment(1);
        context.getCounter("E3", "OFFERS_PER_CITY_TOTAL").increment(n);

        for (int i = 0; i < 6; i++) {
            double rM2 = 0.0, rCal = 0.0, rPow = 0.0;
            
            if (M2_X[i] > 0) {
                if (M2_Y[0] > 0) rM2 = covXY[i][0] / Math.sqrt(M2_X[i] * M2_Y[0]);
                if (M2_Y[1] > 0) rCal = covXY[i][1] / Math.sqrt(M2_X[i] * M2_Y[1]);
                if (M2_Y[2] > 0) rPow = covXY[i][2] / Math.sqrt(M2_X[i] * M2_Y[2]);
            }

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

    private static String fmt(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return "0.000000";
        return String.format(java.util.Locale.US, "%.6f", v);
    }
}
