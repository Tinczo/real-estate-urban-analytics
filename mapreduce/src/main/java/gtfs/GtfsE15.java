package gtfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class GtfsE15 {

    // args: <input> <output> [numReducers] [withCombiner]
    // numReducers: default 1
    // withCombiner: "true" to enable, default false
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Użycie: GtfsE15 <input> <output> [numReducers] [withCombiner]");
            System.exit(1);
        }
        int numReducers = args.length >= 3 ? Integer.parseInt(args[2]) : 1;
        boolean withCombiner = args.length >= 4 && "true".equalsIgnoreCase(args[3]);

        Configuration conf15 = new Configuration();
        Job job15 = Job.getInstance(conf15,
                "E1.5 GTFS Stop Score [reducers=" + numReducers + " combiner=" + withCombiner + "]");
        job15.setJarByClass(GtfsE15.class);

        job15.setMapperClass(GtfsAggregationMapper.class);
        job15.setReducerClass(GtfsAggregationReducer.class);
        job15.setNumReduceTasks(numReducers);

        if (withCombiner) {
            job15.setCombinerClass(GtfsCombiner.class);
        }

        job15.setOutputKeyClass(Text.class);
        job15.setOutputValueClass(StopInfo.class);

        job15.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job15, new Path(args[0]));
        FileOutputFormat.setOutputPath(job15, new Path(args[1]));

        if (!job15.waitForCompletion(true)) System.exit(1);
    }
}
