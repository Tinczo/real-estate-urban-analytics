package gtfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class E2Driver {

    // args: <t1_5_dir> <t1_2_dir> <t1_3_dir> <output_t2> [walkingRadiusM]
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Użycie: E2Driver <t1_5> <t1_2> <t1_3> <output_t2> [radiusM]");
            System.exit(1);
        }

        double radius = args.length >= 5 ? Double.parseDouble(args[4]) : 800.0;

        Configuration conf = new Configuration();
        conf.setDouble("pipeline.walking.radius.m", radius);

        Job job = Job.getInstance(conf,
                "E2 Indicators per Property [radius=" + (int) radius + "m]");
        job.setJarByClass(E2Driver.class);

        // MultipleInputs: każde źródło ma własny Mapper
        MultipleInputs.addInputPath(job, new Path(args[0]),
                SequenceFileInputFormat.class, JoinMapperT15.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                SequenceFileInputFormat.class, JoinMapperT12.class);
        MultipleInputs.addInputPath(job, new Path(args[2]),
                SequenceFileInputFormat.class, JoinMapperT13.class);

        // Wspólne typy wyjścia Mappera (wymagane przy MultipleInputs)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinRecord.class);

        job.setReducerClass(E2Reducer.class);
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        if (!job.waitForCompletion(true)) System.exit(1);
    }
}
