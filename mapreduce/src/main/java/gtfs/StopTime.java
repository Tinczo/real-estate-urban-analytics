package gtfs;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StopTime implements Writable {

    private String stopId;
    private double lat;
    private double lon;
    private String geoId;
    private String routeType;
    private String arrivalTime;

    public StopTime() {}


    public void set(String stopId, double lat, double lon, String geoId, String routeType, String arrivalTime) {
        this.stopId = stopId;
        this.lat = lat;
        this.lon = lon;
        this.geoId = geoId;
        this.routeType = routeType;
        this.arrivalTime = arrivalTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(stopId);
        out.writeDouble(lat);
        out.writeDouble(lon);
        out.writeUTF(geoId);
        out.writeUTF(routeType);
        out.writeUTF(arrivalTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.stopId = in.readUTF();
        this.lat = in.readDouble();
        this.lon = in.readDouble();
        this.geoId = in.readUTF();
        this.routeType = in.readUTF();
        this.arrivalTime = in.readUTF();
    }

    @Override
    public String toString() {
        return stopId + "|" + lat + "|" + lon + "|" + geoId + "|" + routeType + "|" + arrivalTime;
    }
}