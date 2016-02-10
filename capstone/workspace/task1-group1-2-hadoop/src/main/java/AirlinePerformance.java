import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import static java.lang.System.exit;
import static org.apache.hadoop.mapreduce.Job.getInstance;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class AirlinePerformance {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = getInstance(conf, "Airlines performance");

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(PerformanceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        addInputPath(job, new Path(args[0]));
        setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(10);

        job.setJarByClass(AirlinePerformance.class);
        exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
