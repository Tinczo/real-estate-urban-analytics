import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class GtfsJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Map<String, String>   tripToRoute    = new HashMap<>();
    private final Map<String, String>   tripToService  = new HashMap<>();
    private final Map<String, String>   routeToType    = new HashMap<>();
    private final Set<String>           workdayService = new HashSet<>();
    private final Map<String, StopInfo> stopInfo       = new HashMap<>();

    private Text outKey   = new Text();
    private Text outValue = new Text();

    private FileSystem fs;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        fs = FileSystem.get(conf);

        URI[] caches = context.getCacheFiles();

        if (caches == null || caches.length == 0) {
            System.err.println("[WARN] Brak plików w Distributed Cache!");
            return;
        }

        for (URI uri : caches) {
            String name = new Path(uri).getName();

            switch (name) {
                case "trips.txt":
                    loadTrips(uri);
                    break;
                case "routes.txt":
                    loadRoutes(uri);
                    break;
                case "calendar.txt":
                    loadCalendar(uri);
                    break;
                case "stops.txt":
                    loadStops(uri);
                    break;
            }
        }
    }

    private void loadTrips(URI uri) throws IOException {
        try (BufferedReader br = newReader(uri)) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                String[] f = line.split(",", -1);
                if (f.length < 3) continue;
                tripToRoute.put(f[2].trim(), f[0].trim());   // trip_id → route_id
                tripToService.put(f[2].trim(), f[1].trim()); // trip_id → service_id
            }
        }
    }

    private void loadRoutes(URI uri) throws IOException {
        try (BufferedReader br = newReader(uri)) {
            String line;
            boolean first = true;
            int[] idx = null;
            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    idx = headerIndex(line, "route_id", "route_type");
                    continue;
                }
                String[] f = line.split(",", -1);
                if (f.length <= Math.max(idx[0], idx[1])) continue;
                routeToType.put(f[idx[0]].trim(), f[idx[1]].trim());
            }
        }
    }

    private void loadCalendar(URI uri) throws IOException {
        try (BufferedReader br = newReader(uri)) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                String[] f = line.split(",", -1);
                if (f.length < 8) continue;
                String svcId = f[0].trim();
                boolean isWorkday = f[1].equals("1") || f[2].equals("1")
                        || f[3].equals("1") || f[4].equals("1")
                        || f[5].equals("1");
                if (isWorkday) workdayService.add(svcId);
            }
        }
    }

    private void loadStops(URI uri) throws IOException {
        try (BufferedReader br = newReader(uri)) {
            String line;
            boolean first = true;
            int[] idx = null;
            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    idx = headerIndex(line, "stop_id", "stop_lat", "stop_lon");
                    continue;
                }
                String[] f = line.split(",", -1);
                try {
                    String stopId = f[idx[0]].trim();
                    double lat    = Double.parseDouble(f[idx[1]].trim());
                    double lon    = Double.parseDouble(f[idx[2]].trim());
                    stopInfo.put(stopId, new StopInfo(lat, lon));
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException ignored) {}
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.startsWith("trip_id")) return;

        String[] f = line.split(",", -1);
        if (f.length < 4) return;

        String tripId      = f[0].trim();
        String arrivalTime = f[1].trim();
        String stopId      = f[3].trim();

        String routeId = tripToRoute.get(tripId);
        if (routeId == null) return;

        String routeType = routeToType.getOrDefault(routeId, "-1");

        String serviceId = tripToService.get(tripId);
        if (serviceId != null && !workdayService.contains(serviceId)) return;

        StopInfo stop = stopInfo.get(stopId);
        if (stop == null) return;

        outKey.set(stop.geoId);
        outValue.set(String.join("|",
                stopId,
                String.valueOf(stop.lat),
                String.valueOf(stop.lon),
                stop.geoId,
                routeType,
                arrivalTime));

        context.write(outKey, outValue);
    }

    private BufferedReader newReader(URI uri) throws IOException {
        Path path = new Path(uri);
        return new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8));
    }

    private int[] headerIndex(String headerLine, String... names) {
        headerLine = headerLine.replace("\uFEFF", "");

        String[] cols = headerLine.split(",", -1);
        int[] idx = new int[names.length];
        Map<String, Integer> pos = new HashMap<>();

        for (int i = 0; i < cols.length; i++) {
            String cleanColName = cols[i].trim().replaceAll("^\"|\"$", "");
            pos.put(cleanColName, i);
        }

        for (int i = 0; i < names.length; i++) {
            idx[i] = pos.getOrDefault(names[i], -1);
        }
        return idx;
    }
}