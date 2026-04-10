package gtfs;

import com.github.davidmoten.geo.GeoHash;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StopInfo implements Writable {
    public double lat;
    public double lon;
    public String geoId;
    public String stopId;
    public double score;

    public StopInfo() {}

    public StopInfo(double lat, double lon) {
        this.lat   = lat;
        this.lon   = lon;
        this.geoId = GeoHash.encodeHash(lat, lon, 6);
    }

    public void setForCount(String stopId, double lat, double lon) {
        this.stopId = stopId;
        this.lat = lat;
        this.lon = lon;
        this.geoId = GeoHash.encodeHash(lat, lon, 6);
        this.score = 1.0; // W Mapperze to jest nasze "1" przejazd
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(lat);
        out.writeDouble(lon);
        out.writeUTF(geoId);
        out.writeUTF(stopId);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.lat = in.readDouble();
        this.lon = in.readDouble();
        this.geoId = in.readUTF();
        this.stopId = in.readUTF();
        this.score = in.readDouble();
    }
}