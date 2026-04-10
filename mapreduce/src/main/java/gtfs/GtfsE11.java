package gtfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.net.URI;

public class GtfsE11 {

    public static void main(String[] args) throws Exception {
        // args: [0]=stop_times  [1]=cache_dir  [2]=out_e11

        Configuration conf11 = new Configuration();
        Job job11 = Job.getInstance(conf11, "E1.1 GTFS Extraction + Map Side Join");

        job11.setJarByClass(GtfsE11.class);
        job11.setMapperClass(GtfsJoinMapper.class);

        job11.setNumReduceTasks(0);

        job11.setOutputKeyClass(Text.class);
        job11.setOutputValueClass(StopTime.class);
        job11.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job11, new Path(args[0])); // stop_times.txt
        FileOutputFormat.setOutputPath(job11, new Path(args[2]));

        // ── Distributed Cache: małe pliki GTFS ───────────────────────────────
        String cacheBase = args[1];
        job11.addCacheFile(new URI(cacheBase + "/trips.txt#trips.txt"));
        job11.addCacheFile(new URI(cacheBase + "/routes.txt#routes.txt"));
        job11.addCacheFile(new URI(cacheBase + "/calendar.txt#calendar.txt"));
        job11.addCacheFile(new URI(cacheBase + "/stops.txt#stops.txt"));

        if (!job11.waitForCompletion(true)) System.exit(1);
    }
}