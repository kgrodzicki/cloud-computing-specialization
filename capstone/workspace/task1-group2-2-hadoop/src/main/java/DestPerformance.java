import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import static java.lang.System.exit;
import static org.apache.hadoop.mapreduce.Job.getInstance;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class DestPerformance {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/task/tmp");
        fs.delete(tmpPath, true);

        Job job_1 = getInstance(conf, "Dest performance");

        job_1.setMapperClass(TokenizerMapper.class);
        job_1.setReducerClass(PerformanceReducer.class);

        job_1.setMapOutputKeyClass(Text.class);
        job_1.setMapOutputValueClass(IntWritable.class);
        job_1.setOutputKeyClass(Text.class);
        job_1.setOutputValueClass(Text.class);

        addInputPath(job_1, new Path(args[0]));
        setOutputPath(job_1, tmpPath);

        job_1.setJarByClass(DestPerformance.class);
        job_1.waitForCompletion(true);

        Job job_2 = getInstance(conf, "Top Dest");

        job_2.setMapperClass(TopDestMapper.class);
        job_2.setReducerClass(TopDestReducer.class);

        job_2.setMapOutputKeyClass(Text.class);
        job_2.setMapOutputValueClass(TextArrayWritable.class);

        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(Text.class);
        job_2.setOutputFormatClass(NullOutputFormat.class);

        addInputPath(job_2, tmpPath);

        job_2.setJarByClass(DestPerformance.class);
        job_2.setNumReduceTasks(20);

        exit(job_2.waitForCompletion(true) ? 0 : 1);
    }
}
