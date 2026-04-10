package gtfs;

import com.github.davidmoten.geo.GeoHash;

public class StopInfo {
    public final double lat;
    public final double lon;
    public final String geoId;

    public StopInfo(double lat, double lon) {
        this.lat   = lat;
        this.lon   = lon;
        this.geoId = GeoHash.encodeHash(lat, lon, 6);
    }
}