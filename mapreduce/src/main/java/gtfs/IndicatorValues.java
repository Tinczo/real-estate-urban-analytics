package gtfs;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Jeden wiersz T2 jako Writable — przekazywany z Mappera E3 do Reducera E3.
 */
public class IndicatorValues implements Writable {
    public double cenaZaM2;
    public double cenaCalkowita;
    public double powierzchnia;
    public double wskPrzep;
    public double indHandlu;
    public double indGastro;
    public double indRekr;
    public double indEduk;
    public double wspHalas;

    public IndicatorValues() {}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(cenaZaM2);
        out.writeDouble(cenaCalkowita);
        out.writeDouble(powierzchnia);
        out.writeDouble(wskPrzep);
        out.writeDouble(indHandlu);
        out.writeDouble(indGastro);
        out.writeDouble(indRekr);
        out.writeDouble(indEduk);
        out.writeDouble(wspHalas);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.cenaZaM2      = in.readDouble();
        this.cenaCalkowita = in.readDouble();
        this.powierzchnia  = in.readDouble();
        this.wskPrzep      = in.readDouble();
        this.indHandlu     = in.readDouble();
        this.indGastro     = in.readDouble();
        this.indRekr       = in.readDouble();
        this.indEduk       = in.readDouble();
        this.wspHalas      = in.readDouble();
    }
}
