package gtfs;

import com.github.davidmoten.geo.GeoHash;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OsmRecord implements Writable {
    public String geoId;
    public double lat;
    public double lon;
    public String category;  // HANDEL | GASTRO | REKREACJA | EDUKACJA | DROGA
    public double wagaObiektu;

    public OsmRecord() {}

    public void set(double lat, double lon, String category, double wagaObiektu) {
        this.lat = lat;
        this.lon = lon;
        this.geoId = GeoHash.encodeHash(lat, lon, 6);
        this.category = category;
        this.wagaObiektu = wagaObiektu;
    }

    @Override
    public String toString() {
        return geoId + "," + lat + "," + lon + "," + category + "," + wagaObiektu;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(geoId == null ? "" : geoId);
        out.writeDouble(lat);
        out.writeDouble(lon);
        out.writeUTF(category == null ? "" : category);
        out.writeDouble(wagaObiektu);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.geoId = in.readUTF();
        this.lat = in.readDouble();
        this.lon = in.readDouble();
        this.category = in.readUTF();
        this.wagaObiektu = in.readDouble();
    }
}
