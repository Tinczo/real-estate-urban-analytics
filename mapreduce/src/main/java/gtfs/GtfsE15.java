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

    public static void main(String[] args) throws Exception {
        Configuration conf15 = new  Configuration();
        Job job15 =  Job.getInstance(conf15, "E1.1 GTFS Extraction");
        job15.setJarByClass(GtfsE15.class);

        job15.setMapperClass(GtfsAggregationMapper.class);
        job15.setReducerClass(GtfsAggregationReducer.class);

        job15.setOutputKeyClass(Text.class);
        job15.setOutputValueClass(StopInfo.class);

        job15.setInputFormatClass(SequenceFileInputFormat.class);
        job15.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job15, new Path(args[0]));
        FileOutputFormat.setOutputPath(job15, new Path(args[1]));

        if (!job15.waitForCompletion(true)) System.exit(1);

    }
}
