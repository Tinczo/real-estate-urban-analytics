package gtfs;

import com.github.davidmoten.geo.GeoHash;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RealEstateRecord implements Writable {
    public String idOferty;
    public String geoId;
    public double lat;
    public double lon;
    public double cenaZaM2;
    public double cenaCalkowita;
    public double powierzchnia;
    public String miasto;

    public RealEstateRecord() {}

    public void set(String idOferty, double lat, double lon,
                    double cenaZaM2, double cenaCalkowita,
                    double powierzchnia, String miasto) {
        this.idOferty = idOferty;
        this.lat = lat;
        this.lon = lon;
        this.geoId = GeoHash.encodeHash(lat, lon, 6);
        this.cenaZaM2 = cenaZaM2;
        this.cenaCalkowita = cenaCalkowita;
        this.powierzchnia = powierzchnia;
        this.miasto = miasto;
    }

    @Override
    public String toString() {
        return idOferty + "," + geoId + "," + lat + "," + lon + ","
                + cenaZaM2 + "," + cenaCalkowita + "," + powierzchnia + "," + miasto;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(idOferty == null ? "" : idOferty);
        out.writeUTF(geoId == null ? "" : geoId);
        out.writeDouble(lat);
        out.writeDouble(lon);
        out.writeDouble(cenaZaM2);
        out.writeDouble(cenaCalkowita);
        out.writeDouble(powierzchnia);
        out.writeUTF(miasto == null ? "" : miasto);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.idOferty = in.readUTF();
        this.geoId = in.readUTF();
        this.lat = in.readDouble();
        this.lon = in.readDouble();
        this.cenaZaM2 = in.readDouble();
        this.cenaCalkowita = in.readDouble();
        this.powierzchnia = in.readDouble();
        this.miasto = in.readUTF();
    }
}
