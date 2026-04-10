package gtfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalTime;

public class GtfsAggregationMapper extends Mapper<Text, StopTime, Text, StopInfo> {

    private final Text outKey = new Text();
    private final StopInfo stopInfoVal = new StopInfo();

    private final LocalTime start = LocalTime.of(7, 0);
    private final LocalTime end = LocalTime.of(9, 0);

    @Override
    protected void map(Text key, gtfs.StopTime value, Context context) throws IOException, InterruptedException {
        String arrivalTimeStr = value.getArrivalTime();
        int hour = Integer.parseInt(arrivalTimeStr.split(":")[0]);
        if (hour > 9 || hour < 7) return;

        outKey.set(value.getStopId());
        stopInfoVal.setForCount(value.getStopId(), value.getLat(), value.getLon());
        context.write(outKey, stopInfoVal);
    }
}