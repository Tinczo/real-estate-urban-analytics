package gtfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class E3Driver {

    // args: <input_t2_dir> <output_t3>
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Użycie: E3Driver <input_t2> <output_t3>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "E3 Pearson Correlation Matrix");
        job.setJarByClass(E3Driver.class);

        job.setMapperClass(E3Mapper.class);
        job.setReducerClass(E3Reducer.class);

        // Jeden reducer — upraszcza zbieranie wyników per miasto,
        // ale hash partitioner zapewni poprawne grupowanie przy >1 reducerze też.
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IndicatorValues.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) System.exit(1);
    }
}
