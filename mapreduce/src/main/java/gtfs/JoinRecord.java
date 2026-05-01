package gtfs;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Tagged-union Writable łączący rekordy z T1.5, T1.2 i T1.3 dla E2 Reduce-side Join.
 * Wszystkie pola są zawsze serializowane (flat struct) dla prostoty.
 */
public class JoinRecord implements Writable {

    public static final byte SOURCE_T15 = 1;  // przystanek GTFS
    public static final byte SOURCE_T12 = 2;  // obiekt OSM
    public static final byte SOURCE_T13 = 3;  // oferta nieruchomości

    public byte source;

    // T1.5 — przystanek
    public double stopLat;
    public double stopLon;
    public double kK;

    // T1.2 — POI OSM
    public double poiLat;
    public double poiLon;
    public String category;
    public double wagaObiektu;

    // T1.3 — oferta nieruchomości
    public String idOferty;
    public String miasto;
    public double offerLat;
    public double offerLon;
    public double cenaZaM2;
    public double cenaCalkowita;
    public double powierzchnia;

    public JoinRecord() {
        category = "";
        idOferty = "";
        miasto   = "";
    }

    public void copyFrom(JoinRecord other) {
        this.source       = other.source;
        this.stopLat      = other.stopLat;
        this.stopLon      = other.stopLon;
        this.kK           = other.kK;
        this.poiLat       = other.poiLat;
        this.poiLon       = other.poiLon;
        this.category     = other.category;
        this.wagaObiektu  = other.wagaObiektu;
        this.idOferty     = other.idOferty;
        this.miasto       = other.miasto;
        this.offerLat     = other.offerLat;
        this.offerLon     = other.offerLon;
        this.cenaZaM2     = other.cenaZaM2;
        this.cenaCalkowita = other.cenaCalkowita;
        this.powierzchnia = other.powierzchnia;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(source);
        out.writeDouble(stopLat);
        out.writeDouble(stopLon);
        out.writeDouble(kK);
        out.writeDouble(poiLat);
        out.writeDouble(poiLon);
        out.writeUTF(category);
        out.writeDouble(wagaObiektu);
        out.writeUTF(idOferty);
        out.writeUTF(miasto);
        out.writeDouble(offerLat);
        out.writeDouble(offerLon);
        out.writeDouble(cenaZaM2);
        out.writeDouble(cenaCalkowita);
        out.writeDouble(powierzchnia);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.source        = in.readByte();
        this.stopLat       = in.readDouble();
        this.stopLon       = in.readDouble();
        this.kK            = in.readDouble();
        this.poiLat        = in.readDouble();
        this.poiLon        = in.readDouble();
        this.category      = in.readUTF();
        this.wagaObiektu   = in.readDouble();
        this.idOferty      = in.readUTF();
        this.miasto        = in.readUTF();
        this.offerLat      = in.readDouble();
        this.offerLon      = in.readDouble();
        this.cenaZaM2      = in.readDouble();
        this.cenaCalkowita = in.readDouble();
        this.powierzchnia  = in.readDouble();
    }
}
